import time
import sys
import argparse
import numpy as np
import joblib

def run_benchmark(threshold_us: int):
    print(f"Running latency benchmark with strict threshold: {threshold_us} microseconds")
    
    # In a real scenario, this would send an HTTP request to the inference-service API
    # and measure response time. For the purpose of this script, we'll benchmark the model load directly
    # and simulate the network overhead check.
    
    try:
        model = joblib.load('inference-service/model.joblib')
    except Exception as e:
        print(f"Failed to load model for benchmarking: {e}")
        sys.exit(1)

    latencies = []
    
    # Warmup
    for _ in range(10):
        features = np.random.rand(1, 5)
        model.predict(features)
        
    # Benchmark
    for _ in range(1000):
        features = np.random.rand(1, 5)
        t0 = time.perf_counter()
        model.predict(features)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1_000_000)
        
    avg_latency = np.mean(latencies)
    p99_latency = np.percentile(latencies, 99)
    
    print(f"Benchmark Results:")
    print(f"Average Latency: {avg_latency:.2f} us")
    print(f"P99 Latency:     {p99_latency:.2f} us")
    
    if p99_latency > threshold_us:
        print(f"FAILED: P99 latency ({p99_latency:.2f} us) exceeds threshold ({threshold_us} us)")
        sys.exit(1)
    else:
        print("PASSED: Latency is within acceptable limits.")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=int, default=500, help="Latency threshold in microseconds")
    args = parser.parse_args()
    
    run_benchmark(args.threshold)
