import json
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import joblib
import shap
from kafka import KafkaConsumer

class FraudAutoencoder(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.encoder = nn.Sequential(nn.Linear(input_dim, 64), nn.ReLU(), nn.Linear(64, 32), nn.ReLU(), nn.Linear(32, 16))
        self.decoder = nn.Sequential(nn.Linear(16, 32), nn.ReLU(), nn.Linear(32, 64), nn.ReLU(), nn.Linear(64, input_dim))
    def forward(self, x): return self.decoder(self.encoder(x))

class CostSensitiveClassifier(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, 64), nn.ReLU(), nn.Dropout(0.3),
            nn.Linear(64, 32), nn.ReLU(), nn.Dropout(0.2),
            nn.Linear(32, 1)
        )
    def forward(self, x): return self.network(x)

class FraudScoringEngine:
    def __init__(self):
        print("Initializing Fraud Scoring Engine...")
        self.device = torch.device("cpu")
        
        # Load Artifacts
        self.scaler = joblib.load('models/feature_scaler.pkl')
        self.feature_names = joblib.load('models/feature_names.pkl')
        input_dim = len(self.feature_names)
        
        # Load Models
        self.autoencoder = FraudAutoencoder(input_dim).to(self.device)
        self.autoencoder.load_state_dict(torch.load('models/best_autoencoder_weights.pth', map_location=self.device, weights_only=True))
        self.autoencoder.eval()
        
        self.classifier = CostSensitiveClassifier(input_dim).to(self.device)
        self.classifier.load_state_dict(torch.load('models/best_classifier_weights.pth', map_location=self.device, weights_only=True))
        self.classifier.eval()
        
        # SHAP Initialization
        background = pd.read_csv('models/shap_background.csv').values
        background_tensor = torch.FloatTensor(background).to(self.device)
        self.explainer = shap.DeepExplainer(self.classifier, background_tensor)

        # Kafka Consumer
        self.consumer = KafkaConsumer(
            'financial_applications',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest'
        )

    def process_stream(self):
        print("Listening for incoming financial applications...")
        flagged_db = []
        for message in self.consumer:
            app_id = message.value['app_id']
            features = message.value['features']
            
            # Format and Scale
            df = pd.DataFrame([features])
            df = df.reindex(columns=self.feature_names, fill_value=0)
            x_scaled = self.scaler.transform(df)
            x_tensor = torch.FloatTensor(x_scaled).to(self.device)
            
            with torch.no_grad():
                # 1. Cost-Sensitive Probabilities
                logits = self.classifier(x_tensor)
                prob = torch.sigmoid(logits).item()
                
                # 2. Autoencoder Anomaly Score
                recon = self.autoencoder(x_tensor)
                mse_loss = torch.mean((x_tensor - recon)**2).item()
            
            print(f"Scored {app_id} | Prob: {prob:.4f} | MSE: {mse_loss:.4f}")
            
            # 3. Flagging & SHAP Generation (Optimized Thresholds)
            if prob > 0.9168 or mse_loss > 0.5159:
                print(f"⚠️ FRAUD DETECTED: {app_id} - Generating SHAP Explanations...")
                shap_values = self.explainer.shap_values(x_tensor)
                
                flagged_db.append({
                    "app_id": app_id,
                    "prob": prob,
                    "mse": mse_loss,
                    "shap_values": shap_values[0].tolist(),
                    "features": features
                })
                
                with open('flagged_applications.json', 'w') as f:
                    json.dump(flagged_db, f)

if __name__ == "__main__":
    engine = FraudScoringEngine()
    engine.process_stream()


    