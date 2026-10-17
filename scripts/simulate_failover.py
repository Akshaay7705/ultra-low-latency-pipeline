import os
import time
import subprocess
import random

NAMESPACES = ["ml-pipeline-us-east", "ml-pipeline-us-west"]
SERVICES = ["inference-service", "feature-service", "ingestion-service"]

def get_pods(namespace, service):
    cmd = f"kubectl get pods -n {namespace} -l app={service} -o jsonpath='{{.items[*].metadata.name}}'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0 and result.stdout:
        return result.stdout.split()
    return []

def delete_pod(namespace, pod_name):
    print(f"[*] Simulating node/pod failure: Deleting {pod_name} in {namespace}...")
    cmd = f"kubectl delete pod {pod_name} -n {namespace} --grace-period=0 --force"
    subprocess.run(cmd, shell=True, capture_output=True)

def simulate_failover():
    print("=== Active-Active Failover Simulation ===")
    
    while True:
        try:
            ns = random.choice(NAMESPACES)
            svc = random.choice(SERVICES)
            
            pods = get_pods(ns, svc)
            if pods:
                target_pod = random.choice(pods)
                delete_pod(ns, target_pod)
                
                print(f"[*] Kubernetes ReplicaSet will automatically spin up a replacement.")
                print(f"[*] Kafka consumers in the consumer group will automatically rebalance.")
                print("[*] Waiting for 5 seconds before next chaos event...\n")
            else:
                print(f"[!] No pods found for {svc} in {ns}. Skipping.")
                
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\nStopping failover simulation.")
            break

if __name__ == "__main__":
    simulate_failover()
