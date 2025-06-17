import time
import datetime
import numpy as np
import threading
import json
import websocket
import sqlite3
from collections import deque
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

API_TOKEN = "REzKac9b5BR7DmF"
APP_ID = "71130"
SYMBOL = "gold"
TRADE_DURATION = '1h'

# ========== Fanomanana ==========
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

# ========== Fanamboarana Database ==========
conn = sqlite3.connect("trading_log.db", check_same_thread=False)
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

# ========== Fanamboarana WebSocket Deriv ==========
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
                direction = get_candle_analysis([{}, data])
                c.execute("INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (candle['epoch'], tf, data['open'], data['high'], data['low'], data['close'], data['volume_tick'], data['volume_real'], direction))
                conn.commit()
                print(f"[DATA] {tf}: {data}")
            elif msg['msg_type'] == 'buy':
                print(f"[TRADE CONFIRMED] {msg['buy']['transaction_id']}")
                c.execute("INSERT INTO trades VALUES (?, ?, ?)", (int(time.time()), msg['buy']['contract_type'], 'pending'))
                conn.commit()

    def on_error(self, ws, error):
        print(f"[WS ERROR] {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("[WS] Connection closed")

    def run(self):
        threading.Thread(target=self.ws.run_forever).start()
        while not self.authorized:
            time.sleep(1)

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

# ========== Logique d'analyse ==========
def get_candle_analysis(candles):
    if len(candles) < 2:
        return None
    last = candles[-1]
    vol_tick = last['volume_tick']
    vol_real = last['volume_real']
    bullish = last['close'] > last['open']
    bearish = last['close'] < last['open']

    if vol_real > vol_tick:
        if bearish:
            return 'sell'
        elif bullish:
            return 'buy'
    else:
        if bullish:
            return 'sell'
        elif bearish:
            return 'buy'
    return None

def multitimeframe_analysis():
    results = {}
    for tf in TIMEFRAMES:
        results[tf] = get_candle_analysis(candles_data[tf])
    directions = list(results.values())
    if all(d == 'buy' for d in directions):
        return 'buy'
    elif all(d == 'sell' for d in directions):
        return 'sell'
    return None

# ========== Main Trading Loop ==========
def trading_loop(ws_client):
    print("[LOOP] Starting Trading Loop")
    last_trade_time = 0
    while True:
        now = time.time()
        for tf in TIMEFRAMES:
            ws_client.request_candle(tf)
        if all(len(candles_data[tf]) >= 2 for tf in TIMEFRAMES):
            decision = multitimeframe_analysis()
            if decision and now - last_trade_time >= 3600:
                ws_client.send_trade(decision)
                last_trade_time = now
        time.sleep(10)

# ========== Fanombohana ==========
if __name__ == '__main__':
    deriv_ws = DerivWS(APP_ID, API_TOKEN)
    deriv_ws.run()
    trading_loop(deriv_ws)
