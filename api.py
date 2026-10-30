from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import logging

app = FastAPI(title="Fraud Detection API")

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class ApplicationPayload(BaseModel):
    app_id: str
    features: dict

@app.post("/stream_application")
async def stream_application(payload: ApplicationPayload):
    try:
        producer.send('financial_applications', value=payload.dict())
        return {"status": "200", "message": "Application queued for ML scoring"}
    except Exception as e:
        logging.error(f"Kafka error: {e}")
        raise HTTPException(status_code=500, detail="Message Broker Unavailable")


    