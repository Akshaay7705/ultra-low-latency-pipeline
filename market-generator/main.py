import time
import json
import random
import os
from kafka import KafkaProducer
from typing import Dict, Any

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("TOPIC", "market-ticks")
SYMBOLS = ["AAPL", "TSLA", "GOOG", "MSFT", "AMZN", "META"]

def get_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=0,  # Fire and forget for ultra-low latency simulation
                linger_ms=5, # Batching optimization
                batch_size=16384
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka broker {KAFKA_BROKER}... {e}")
            time.sleep(2)

def generate_tick() -> Dict[str, Any]:
    symbol = random.choice(SYMBOLS)
    price = round(random.uniform(100.0, 500.0), 2)
    volume = random.randint(10, 1000)
    timestamp = time.time()
    
    return {
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "timestamp": timestamp
    }

def main():
    producer = get_producer()
    print(f"Starting market data generation on topic: {TOPIC}")
    
    # Target 1000+ events per second
    # To avoid 100% CPU on single thread but hit high rates, we use small sleeps or tight loop batches
    try:
        while True:
            for _ in range(100):
                tick = generate_tick()
                producer.send(TOPIC, value=tick)
            
            # Flush and sleep a tiny bit to simulate ~1000-5000 msg/sec
            # Time.sleep(0.01) is 10ms, yielding 100 loops/sec * 100 msgs = 10,000 msgs/sec
            time.sleep(0.02)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
