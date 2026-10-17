import os
import json
import time
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict, deque

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "normalized-ticks")
OUT_TOPIC = os.getenv("OUT_TOPIC", "ml-features")

WINDOW_SIZE = 50

class FeatureStore:
    def __init__(self):
        # symbol -> deque of prices
        self.prices = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
        # symbol -> last price
        self.last_price = {}
        
    def update_and_compute(self, data):
        symbol = data["symbol"]
        price = data["normalized_price"]
        
        # update state
        self.prices[symbol].append(price)
        prev_price = self.last_price.get(symbol, price)
        self.last_price[symbol] = price
        
        prices_array = np.array(self.prices[symbol])
        
        # compute features
        moving_average = np.mean(prices_array)
        volatility = np.std(prices_array) if len(prices_array) > 1 else 0.0
        price_delta = price - prev_price
        rolling_mean = np.mean(prices_array[-10:]) if len(prices_array) >= 10 else moving_average
        
        return {
            "symbol": symbol,
            "moving_average": moving_average,
            "volatility": volatility,
            "price_delta": price_delta,
            "rolling_mean": rolling_mean,
            "volume": data["volume"],
            "origin_timestamp": data["origin_timestamp"]
        }

def get_kafka_clients():
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
                group_id="feature-group",
                auto_offset_reset="latest"
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return consumer, producer
        except Exception as e:
            print(f"Waiting for Kafka {KAFKA_BROKER}... {e}")
            time.sleep(2)

def main():
    consumer, producer = get_kafka_clients()
    store = FeatureStore()
    
    print(f"Starting feature engineering from {IN_TOPIC} to {OUT_TOPIC}")
    try:
        for msg in consumer:
            data = msg.value
            features = store.update_and_compute(data)
            
            # send to ML inference service topic
            producer.send(OUT_TOPIC, value=features)
    except KeyboardInterrupt:
        print("Stopping feature service...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()
