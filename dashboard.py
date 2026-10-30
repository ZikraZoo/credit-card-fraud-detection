import streamlit as st
import pandas as pd
import numpy as np
import json
import joblib
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Fraud XAI Dashboard", layout="wide")

st.title("Real-Time Fraud Detection & XAI Audit")

feature_names = joblib.load('models/feature_names.pkl')

if not os.path.exists('flagged_applications.json'):
    st.info("Listening for Kafka stream... No fraudulent applications detected yet.")
    st.stop()

with open('flagged_applications.json', 'r') as f:
    try:
        flagged_data = json.load(f)
    except:
        st.info("Awaiting records...")
        st.stop()

if not flagged_data:
    st.info("Listening for Kafka stream... No fraudulent applications detected yet.")
    st.stop()

st.sidebar.header("Flagged Queue")
app_ids = [record['app_id'] for record in flagged_data]
selected_app = st.sidebar.selectbox("Review Application:", app_ids)

record = next(item for item in flagged_data if item["app_id"] == selected_app)

col1, col2 = st.columns(2)
col1.metric("Fraud Probability (Cost-Sensitive)", f"{record['prob']*100:.2f}%")
col2.metric("Reconstruction Error (Autoencoder)", f"{record['mse']:.4f}")

st.subheader("Regulatory Feature Attribution (SHAP)")

# Array flattened to prevent ValueError
shap_vals = np.array(record['shap_values']).flatten()
shap_series = pd.Series(shap_vals, index=feature_names)

# Get top 10 most impactful features
top_features = shap_series.abs().sort_values(ascending=False).head(10).index
plot_data = shap_series[top_features].sort_values()

fig, ax = plt.subplots(figsize=(10, 5))
colors = ['red' if val > 0 else 'blue' for val in plot_data.values]
plot_data.plot(kind='barh', color=colors, ax=ax)
ax.set_title("Top 10 Fraud Risk Drivers (Red = Increased Risk, Blue = Decreased Risk)")
ax.set_xlabel("SHAP Value (Impact on Model Output)")
st.pyplot(fig)