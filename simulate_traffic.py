import requests
import json
import random
import uuid

API_URL = "http://localhost:8000/stream_application"

def generate_mock_payload(is_fraud=False):
    payload = {
        "app_id": f"APP-{str(uuid.uuid4())[:8].upper()}",
        "features": {
            "income": random.uniform(20000, 150000),
            "name_email_similarity": random.uniform(0.1, 0.9),
            "current_address_months_count": random.randint(1, 120),
            "customer_age": random.randint(18, 80),
            "days_since_request": random.uniform(0, 10)
        }
    }
    
    if is_fraud:
        # Inject anomalous feature behaviors
        payload["features"]["income"] = 999999
        payload["features"]["name_email_similarity"] = 0.01
        payload["features"]["days_since_request"] = 0.001
        
    return payload

print("Initiating simulated financial data stream...")
for i in range(1, 11):
    is_fraud = True if i in [4, 9, 10] else False
    payload = generate_mock_payload(is_fraud=is_fraud)
    
    tag = "🚨 Injecting ANOMALOUS transaction payload..." if is_fraud else "🟢 Sending standard application payload..."
    print(f"\n[{i}/10] {tag}")
    
    response = requests.post(API_URL, json=payload)
    print(f"  -> FastAPI Status: {response.status_code} | {response.json().get('message')}")

print("\nSimulation complete. Review your Streamlit dashboard for flagged applications.")