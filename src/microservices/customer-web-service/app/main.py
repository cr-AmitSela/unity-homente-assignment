from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import os
import requests

app = FastAPI()

# Kafka Producer Class
class KafkaProducerService:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    def publish_message(self, topic, value):
        self.producer.send(topic, value=value)
        self.producer.flush()

# Initialize Kafka Producer globally
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
kafka_topic = 'customer-purchases'

producer = KafkaProducerService(kafka_bootstrap_servers)

# Pydantic Model for Purchase
class Purchase(BaseModel):
    username: str
    userid: int
    price: float
    timestamp: str

# FastAPI Endpoints
@app.post('/buy', status_code=201)
def buy(purchase: Purchase):
    try:
        # Publish purchase to Kafka
        producer.publish_message(kafka_topic, purchase.dict())
        return {"message": "Purchase request published successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/getAllUserBuys')
def get_all_user_buys():
    try:
        # Send GET request to Customer Management Service
        response = requests.get('http://customer-mgmt.customer-mgmt.svc.cluster.local:8080/customer_purchases')
        response.raise_for_status()
        purchases = response.json()
        return purchases
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8081, debug=True)
