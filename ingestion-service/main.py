import os
import json
import asyncio
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel
import time

app = FastAPI(title="Ingestion Service")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "market-ticks")
OUT_TOPIC = os.getenv("OUT_TOPIC", "normalized-ticks")

class Tick(BaseModel):
    symbol: str
    price: float
    volume: int
    timestamp: float

# Global variables for state
consumer = None
producer = None
is_running = False

def get_kafka_clients():
    global consumer, producer
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1
            )
            consumer = KafkaConsumer(
                IN_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="ingestion-group",
                auto_offset_reset="latest"
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            break
        except Exception as e:
            print(f"Waiting for Kafka {KAFKA_BROKER}... {e}")
            time.sleep(2)

def process_messages():
    global is_running
    print(f"Starting ingestion loop from {IN_TOPIC} to {OUT_TOPIC}")
    try:
        for msg in consumer:
            if not is_running:
                break
            try:
                # Validate input
                data = msg.value
                tick = Tick(**data)
                
                # Normalize (e.g. adjust timestamp, calculate pseudo-metrics)
                normalized_data = {
                    "symbol": tick.symbol,
                    "normalized_price": round(tick.price / 100.0, 4), # scale price
                    "volume": tick.volume,
                    "ingest_timestamp": time.time(),
                    "origin_timestamp": tick.timestamp
                }
                
                # Publish
                producer.send(OUT_TOPIC, value=normalized_data)
            except Exception as e:
                # Drop bad messages
                print(f"Validation error: {e}")
    except Exception as e:
        print(f"Consumer loop error: {e}")

@app.on_event("startup")
async def startup_event():
    global is_running
    get_kafka_clients()
    is_running = True
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, process_messages)

@app.on_event("shutdown")
def shutdown_event():
    global is_running
    is_running = False
    if consumer:
        consumer.close()
    if producer:
        producer.close()

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "ingestion-service"}
