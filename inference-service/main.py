import os
import json
import time
import asyncio
import joblib
import redis
import numpy as np
from fastapi import FastAPI
from kafka import KafkaConsumer

app = FastAPI(title="ML Inference Service")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "ml-features")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

MODEL_PATH = os.path.join(os.path.dirname(__file__), "model.joblib")

consumer = None
redis_client = None
model = None
is_running = False

def load_model():
    global model
    try:
        model = joblib.load(MODEL_PATH)
        print("Model loaded successfully.")
    except Exception as e:
        print(f"Error loading model from {MODEL_PATH}: {e}")

def get_clients():
    global consumer, redis_client
    while True:
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
            redis_client.ping()
            
            consumer = KafkaConsumer(
                IN_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="inference-group",
                auto_offset_reset="latest"
            )
            print("Connected to Kafka and Redis.")
            break
        except Exception as e:
            print(f"Waiting for dependencies... {e}")
            time.sleep(2)

def process_messages():
    global is_running
    print(f"Starting inference loop on {IN_TOPIC}")
    
    signals = ["SELL", "HOLD", "BUY"]
    
    try:
        for msg in consumer:
            if not is_running:
                break
            
            data = msg.value
            symbol = data["symbol"]
            
            # extract features in correct order
            features = np.array([[
                data["moving_average"],
                data["volatility"],
                data["price_delta"],
                data["rolling_mean"],
                data["volume"]
            ]])
            
            # measure latency
            t0 = time.perf_counter()
            pred_idx = model.predict(features)[0]
            t1 = time.perf_counter()
            
            latency_us = (t1 - t0) * 1_000_000
            signal = signals[pred_idx]
            
            # store to redis
            end_to_end_latency_ms = (time.time() - data["origin_timestamp"]) * 1000
            
            result = {
                "symbol": symbol,
                "signal": signal,
                "inference_latency_us": round(latency_us, 2),
                "end_to_end_latency_ms": round(end_to_end_latency_ms, 2),
                "timestamp": time.time()
            }
            
            # Save latest signal
            redis_client.set(f"signal:{symbol}", json.dumps(result))
            
            # Track average latency metrics (moving window simulation for dashboard)
            redis_client.lpush("metric:inference_latency_us", latency_us)
            redis_client.ltrim("metric:inference_latency_us", 0, 99) # keep last 100
            
    except Exception as e:
        print(f"Inference loop error: {e}")

@app.on_event("startup")
async def startup_event():
    global is_running
    load_model()
    get_clients()
    is_running = True
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, process_messages)

@app.on_event("shutdown")
def shutdown_event():
    global is_running
    is_running = False
    if consumer:
        consumer.close()

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "inference-service"}

@app.get("/api/signals")
def get_all_signals():
    if not redis_client:
        return {"error": "Redis not connected"}
    
    keys = redis_client.keys("signal:*")
    results = []
    for k in keys:
        val = redis_client.get(k)
        if val:
            results.append(json.loads(val))
    return {"signals": results}
