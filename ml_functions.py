
# ml_function.py
# fetches ml models and generates feature data for them

from config import MODEL_REFRESH_INTERVAL
from data import s3, bucket_name
from tensorflow.keras.models import load_model
import logging, joblib, time, os
logger = logging.getLogger(__name__)

# cache state
cached_models = {}
last_refresh_times = {}

def get_production_data_seconds(client):
    df = client.query_df("""
        SELECT 
            toStartOfInterval(timestamp, INTERVAL 15 SECOND) AS ts, 
            avg(price) AS price
        FROM ticks_db
        WHERE timestamp >= now() - INTERVAL 30 MINUTE
        GROUP BY ts
        ORDER BY ts DESC
    """)
    df = df.sort_index()
    return df

def build_features(df):
    # standard features
    df["returns"] = df["price"].pct_change()
    df["sma_30"] = df["price"].rolling(window=30).mean()
    df["sma_60"] = df["price"].rolling(window=60).mean()
    df["momentum_10"] = df["price"] / df["price"].shift(10) - 1
    df["momentum_30"] = df["price"] / df["price"].shift(30) - 1
    df["volatility_30"] = df["returns"].rolling(window=30).std()
    for lag in [1, 2, 5]:
        df[f"lag_return_{lag}"] = df["returns"].shift(lag)

    # RSI
    delta = df["price"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["14"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = df["price"].ewm(span=12, adjust=False).mean()
    ema26 = df["price"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    
    df = df.dropna()
    return df

class HoldModel:
    def predict(self, X):
        return [1] * len(X)

def get_ml_model(s3_key, local_path):
    now = time.time()

    if (
        s3_key in cached_models and
        (now - last_refresh_times.get(s3_key, 0)) < MODEL_REFRESH_INTERVAL
    ):
        return cached_models[s3_key]

    # otherwise, refresh
    logger.info(f"Downloading model {s3_key} from S3...")
    s3.download_file(bucket_name, s3_key, local_path)

    # choose loader based on file extension for lstm h5
    _, ext = os.path.splitext(local_path)
    if "long_only" in local_path:   # special-case HOLD
        return HoldModel()
    elif ext == ".h5":
        ml_model = load_model(local_path)
    elif ext == ".pkl":
        ml_model = joblib.load(local_path)

    # update cache
    cached_models[s3_key] = ml_model
    last_refresh_times[s3_key] = now

    return ml_model
