# Real-Time Zero-Day Fraud Detection & Explainable AI Pipeline

An end-to-end, high-throughput machine learning pipeline for detecting zero-day bank account fraud in high-dimensional financial applications using the **NeurIPS 2022 Bank Account Fraud (BAF)** dataset.

## System Architecture

```text
[ Application Payload ]
          │
          ▼
   ┌──────────────┐
   │   FastAPI    │ (Asynchronous REST Endpoint)
   └──────┬───────┘
          │ (Produce)
          ▼
   ┌──────────────┐
   │ Apache Kafka │ (Distributed Event Queue - Port 9092)
   └──────┬───────┘
          │ (Consume)
          ▼
 ┌──────────────────┐
 │  PyTorch Scoring │ ───► 1. Unsupervised Autoencoder (Reconstruction Error / Zero-Day)
 │      Engine      │ ───► 2. Cost-Sensitive Network (Weighted BCE Loss)
 └────────┬─────────┘
          │
          ▼ (Flagged Anomalies)
 ┌──────────────────┐
 │  SHAP Explainer  │ (Marginal Feature Attribution)
 └────────┬─────────┘
          │
          ▼
 ┌──────────────────┐
 │ Streamlit Audit  │ (Dynamic Regulatory Compliance Dashboard)
 └──────────────────┘