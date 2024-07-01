from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from pymongo import MongoClient
from kafka import KafkaConsumer
import json
import os
import logging
import threading

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB DAO Class
class CustomerDAO:
    def __init__(self, mongo_uri, database, collection):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database]
        self.collection = self.db[collection]
    
    def insert_purchase(self, purchase_data):
        result = self.collection.insert_one(purchase_data)
        return result.inserted_id
    
    def get_all_purchases(self):
        purchases = list(self.collection.find({}, {'_id': 0}))
        return purchases

# Kafka Consumer Class
class KafkaConsumerService:
    def __init__(self, bootstrap_servers, topic, dao):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.dao = dao

    def consume_messages(self):
        for message in self.consumer:
            try:
                # Assuming message.value is a JSON object representing a Purchase
                purchase_data = message.value
                inserted_id = self.dao.insert_purchase(purchase_data)
                logger.info(f"Inserted purchase {inserted_id}")
            except Exception as e:
                logger.error(f"Error inserting purchase to MongoDB: {e}")

# Initialize DAO and Kafka Consumer globally
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
kafka_topic = 'customer-purchases'

dao = CustomerDAO(mongo_uri, 'mydatabase', 'purchases')
kafka_consumer = KafkaConsumerService(kafka_bootstrap_servers, kafka_topic, dao)

# Start Kafka consumer in a separate thread
def start_kafka_consumer():
    kafka_consumer.consume_messages()

consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
consumer_thread.start()

# Pydantic Model for Purchase
class Purchase(BaseModel):
    username: str
    userid: int
    price: float
    timestamp: str

# FastAPI Endpoints
@app.post('/purchase', status_code=201)
def create_purchase(purchase: Purchase):
    try:
        # Insert purchase into MongoDB
        inserted_id = dao.insert_purchase(purchase.dict())  # Use .dict() to convert to dictionary
        return {"message": "Purchase created successfully", "purchase_id": str(inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/customer_purchases', response_model=List[Purchase])
def get_all_purchases():
    return dao.get_all_purchases()

if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8080, debug=True)
