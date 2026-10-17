import os
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier

def train_dummy_model():
    # Features: moving_average, volatility, price_delta, rolling_mean, volume
    # Generate some random data
    X = np.random.rand(1000, 5)
    # Target: 0 (SELL), 1 (HOLD), 2 (BUY)
    y = np.random.randint(0, 3, 1000)
    
    model = RandomForestClassifier(n_estimators=10, max_depth=5, random_state=42)
    model.fit(X, y)
    
    os.makedirs(os.path.dirname(os.path.abspath(__file__)), exist_ok=True)
    model_path = os.path.join(os.path.dirname(__file__), '..', 'inference-service', 'model.joblib')
    joblib.dump(model, model_path)
    print(f"Dummy model saved to {model_path}")

if __name__ == "__main__":
    train_dummy_model()
