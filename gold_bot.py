# ======= ML-ENHANCED GOLD BOT FULL VERSION (FIXED) =======

import time
import datetime
import numpy as np
import threading
import json
import websocket
import sqlite3
import pandas as pd
import joblib
import schedule
from collections import deque
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

API_TOKEN = "REzKac9b5BR7DmF"
APP_ID = "71130"
SYMBOL = "gold"
TRADE_DURATION = '1h'

TIMEFRAMES = {
    'H1': 3600,
    'M30': 1800,
    'M15': 900
}

candles_data = {
    'H1': deque(maxlen=2),
    'M30': deque(maxlen=2),
    'M15': deque(maxlen=2)
}

# FIX: Add SQLite thread safety
conn = sqlite3.connect("trading_log.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode = WAL")  # Critical for threading
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS candles (
                timestamp INTEGER,
                timeframe TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume_tick INTEGER,
                volume_real INTEGER,
                direction TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS trades (
                timestamp INTEGER,
                direction TEXT,
                result TEXT)''')
conn.commit()

# ========= ML Model Training =========
def train_model():
    print("[ML] Training model...")
    df = pd.read_sql_query("SELECT * FROM candles WHERE direction IN ('buy', 'sell')", conn)
    if len(df) < 100:
        print("[ML] Not enough data to train model.")
        return
    df['target'] = df['direction'].map({'buy': 1, 'sell': 0})
    features = df[['open', 'high', 'low', 'close', 'volume_tick', 'volume_real']]
    target = df['target']
    X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.3, random_state=42)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    joblib.dump(model, "model_rf.pkl")
    acc = accuracy_score(y_test, model.predict(X_test))
    print(f"[ML] Training complete. Accuracy: {acc:.2f}")
    return model  # Return the trained model

# Schedule monthly retraining
schedule.every(30).days.do(train_model)

# FIX: Handle model loading properly
try:
    model = joblib.load("model_rf.pkl")
    print("[ML] Loaded pre-trained model")
except:
    print("[ML] No pre-trained model found. Training new model...")
    model = train_model()

# ========== WebSocket Deriv Class ==========
class DerivWS:
    def __init__(self, app_id, token):
        self.url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
        self.token = token
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.connected = False
        self.msg_queue = []
        self.last_prices = deque(maxlen=100)
        self.authorized = False
        self.trade_ready = False
        self.reconnect_delay = 5  # Seconds between reconnect attempts

    def on_open(self, ws):
        print("[WS] Connected to Deriv WebSocket")
        self.authorize()

    def on_message(self, ws, message):
        msg = json.loads(message)

        if "msg_type" in msg:
            if msg['msg_type'] == 'authorize':
                print("[WS] Authorized")
                self.authorized = True
            elif msg['msg_type'] == 'candles':
                tf = msg['req_id']
                candle = msg['candles'][-1]
                data = {
                    'open': float(candle['open']),
                    'high': float(candle['high']),
                    'low': float(candle['low']),
                    'close': float(candle['close']),
                    'volume_tick': int(candle['epoch'] % 150),
                    'volume_real': int(candle['epoch'] % 250),
                }
                candles_data[tf].append(data)
                
                # FIX: Handle model prediction safely
                if model:
                    direction = predict_direction(data)
                else:
                    if len(candles_data[tf]) >= 2:
                        direction = get_candle_analysis(list(candles_data[tf]))
                    else:
                        direction = None
                
                c.execute("INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (candle['epoch'], tf, data['open'], data['high'], data['low'], 
                           data['close'], data['volume_tick'], data['volume_real'], direction))
                conn.commit()
                print(f"[DATA] {SYMBOL} - {tf}: {data} => {direction}")
            elif msg['msg_type'] == 'buy':
                print(f"[TRADE CONFIRMED] {msg['buy']['transaction_id']}")
                c.execute("INSERT INTO trades VALUES (?, ?, ?)", (int(time.time()), msg['buy']['contract_type'], 'pending'))
                conn.commit()

    def on_error(self, ws, error):
        print(f"[WS ERROR] {error}")
        self.connected = False

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[WS] Connection closed: {close_msg} (code: {close_status_code})")
        self.connected = False
        self.reconnect()

    def reconnect(self):
        print(f"[WS] Reconnecting in {self.reconnect_delay} seconds...")
        time.sleep(self.reconnect_delay)
        self.run()

    def run(self):
        print("[WS] Starting WebSocket connection...")
        self.ws.run_forever()

    def authorize(self):
        self.ws.send(json.dumps({"authorize": API_TOKEN}))

    def request_candle(self, timeframe):
        granularity = TIMEFRAMES[timeframe]
        self.ws.send(json.dumps({
            "ticks_history": SYMBOL,
            "adjust_start_time": 1,
            "count": 2,
            "end": "latest",
            "start": 1,
            "style": "candles",
            "granularity": granularity,
            "req_id": timeframe
        }))

    def send_trade(self, direction):
        contract_type = 'CALL' if direction == 'buy' else 'PUT'
        proposal = {
            "buy": 1,
            "price": 1,
            "parameters": {
                "amount": 1,
                "basis": "stake",
                "contract_type": contract_type,
                "currency": "USD",
                "duration": 1,
                "duration_unit": "h",
                "symbol": SYMBOL.upper()
            }
        }
        self.ws.send(json.dumps(proposal))

# ======= ML Prediction =======
def predict_direction(candle):
    X = [[candle['open'], candle['high'], candle['low'], candle['close'], 
          candle['volume_tick'], candle['volume_real']]]
    try:
        result = model.predict(X)[0]
        return 'buy' if result == 1 else 'sell'
    except Exception as e:
        print(f"[ML PREDICTION ERROR] {e}")
        return None

# ========== Candle Analysis Heuristic ==========
def get_candle_analysis(candles):
    if len(candles) < 2:
        return None
    last = candles[-1]
    prev = candles[-2]
    
    # Improved analysis using both current and previous candle
    bullish = last['close'] > prev['close']
    bearish = last['close'] < prev['close']
    strong_trend = abs(last['close'] - prev['close']) > (prev['high'] - prev['low']) * 0.5

    if bullish and strong_trend and last['volume_real'] > last['volume_tick']:
        return 'buy'
    elif bearish and strong_trend and last['volume_real'] > last['volume_tick']:
        return 'sell'
    return None

# ========== Multi-Timeframe Aggregation ==========
def multitimeframe_analysis():
    results = {}
    for tf in TIMEFRAMES:
        if candles_data[tf]:
            results[tf] = get_candle_analysis(list(candles_data[tf]))
    directions = [d for d in results.values() if d]
    if not directions:
        return None
    if all(d == 'buy' for d in directions):
        return 'buy'
    elif all(d == 'sell' for d in directions):
        return 'sell'
    return None

# FIX: Add schedule runner in background thread
def schedule_runner():
    while True:
        schedule.run_pending()
        time.sleep(60)

# ========== Trading Loop ==========
def trading_loop(ws_client):
    print("[LOOP] Starting Trading Loop")
    last_trade_time = 0
    while True:
        for tf in TIMEFRAMES:
            ws_client.request_candle(tf)
        if all(len(candles_data[tf]) >= 2 for tf in TIMEFRAMES):
            decision = multitimeframe_analysis()
            if decision and time.time() - last_trade_time >= 3600:
                print(f"[TRADE] Decision = {decision}")
                ws_client.send_trade(decision)
                last_trade_time = time.time()
        time.sleep(30)  # More efficient sleep

# ========== Main ==========
if __name__ == '__main__':
    # Start schedule runner in background
    threading.Thread(target=schedule_runner, daemon=True).start()
    
    # Initialize WebSocket
    deriv_ws = DerivWS(APP_ID, API_TOKEN)
    ws_thread = threading.Thread(target=deriv_ws.run, daemon=True)
    ws_thread.start()
    
    # Wait for connection
    while not deriv_ws.authorized:
        print("[MAIN] Waiting for WebSocket authorization...")
        time.sleep(1)
    
    # Start trading loop
    trading_loop(deriv_ws)
