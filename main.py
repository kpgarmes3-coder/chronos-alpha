import asyncio
import json
import httpx
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

app = FastAPI()

# ---------------------------------------------------------
# AI & Crypto Lexicon Engine
# ---------------------------------------------------------
analyzer = SentimentIntensityAnalyzer()
analyzer.lexicon.update({
    'bullish': 2.5, 'bearish': -2.5, 'rekt': -3.5, 
    'moon': 3.0, 'dump': -3.0, 'fud': -2.5, 'pump': 2.5,
    'ath': 2.5, 'hodl': 2.0, 'scam': -4.0, 'liquidation': -3.0
})

class GlobalState:
    def __init__(self):
        self.btc_price = 0.0
        self.order_book_imbalance = 0.0
        self.latest_liquidation = "None"
        self.fng_value = 50
        self.fng_status = "Neutral"
        self.reddit_posts = []
        self.sentiment_score = 0.0

state = GlobalState()

# ---------------------------------------------------------
# Binance Master Stream Worker
# ---------------------------------------------------------
async def binance_master_worker():
    streams = ['btcusdt@aggTrade', 'btcusdt@depth5']
    ws_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
    
    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                print("🟢 Chronos Master Stream Connected!")
                while True:
                    msg = await ws.recv()
                    response = json.loads(msg)
                    stream_name = response.get('stream')
                    payload = response.get('data')
                    
                    if stream_name == 'btcusdt@aggTrade':
                        state.btc_price = float(payload['p'])
                        if float(payload['q']) > 1.0:
                            side = "SELL 🩸" if payload['m'] else "BUY 🚀"
                            state.latest_liquidation = f"Whale {side} {payload['q']} BTC @ {payload['p']}"
                            
                    elif stream_name == 'btcusdt@depth5':
                        bids_sum = sum([float(b[1]) for b in payload['bids']])
                        asks_sum = sum([float(a[1]) for a in payload['asks']])
                        total = bids_sum + asks_sum
                        if total > 0:
                            state.order_book_imbalance = (bids_sum - asks_sum) / total
                            
        except Exception as e:
            print(f"🔴 Stream Disconnected ({e}). Reconnecting in 3s...")
            await asyncio.sleep(3)

# ---------------------------------------------------------
# Macro & Sentiment Worker
# ---------------------------------------------------------
async def fetch_macro_and_sentiment():
    headers = {'User-Agent': 'ChronosAlpha-Apex/2.0'}
    while True:
        try:
            async with httpx.AsyncClient() as client:
                fng_res = await client.get("https://api.alternative.me/fng/")
                fng_data = fng_res.json()['data'][0]
                state.fng_value = int(fng_data['value'])
                state.fng_status = fng_data['value_classification']

                reddit_res = await client.get("https://www.reddit.com/r/CryptoCurrency/new.json?limit=6", headers=headers)
                posts = reddit_res.json()['data']['children']
                
                parsed, total_comp = [], 0
                for post in posts:
                    d = post['data']
                    title = d['title']
                    vs = analyzer.polarity_scores(title)
                    total_comp += vs['compound']
                    
                    status = "Bullish" if vs['compound'] >= 0.05 else "Bearish" if vs['compound'] <= -0.05 else "Neutral"
                    parsed.append({
                        "title": title, "author": d['author'], 
                        "upvotes": d['score'], "sentiment": status, "compound": vs['compound']
                    })
                
                state.reddit_posts = parsed
                state.sentiment_score = total_comp / len(posts) if posts else 0
                print("⚡ Apex Engine: Macro & NLP Synced.")
        except Exception as e:
            print(f"⚠️ Apex Sync Error: {e}")
            
        await asyncio.sleep(300)

@app.on_event("startup")
async def startup():
    asyncio.create_task(binance_master_worker())
    asyncio.create_task(fetch_macro_and_sentiment())

# ---------------------------------------------------------
# WebSocket Endpoint for Frontend
# ---------------------------------------------------------
@app.websocket("/ws/apex")
async def apex_websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            norm_fng = (state.fng_value - 50) / 50
            ai_score = (state.order_book_imbalance * 0.3) + (state.sentiment_score * 0.4) + (norm_fng * 0.3)
            
            direction = "STRONG BULLISH 🚀" if ai_score > 0.15 else "STRONG BEARISH 🩸" if ai_score < -0.15 else "CONSOLIDATION ⚖️"
            confidence = min(abs(ai_score) * 100 + 50, 99.4)

            payload = {
                "btc_price": state.btc_price,
                "order_imbalance": round(state.order_book_imbalance * 100, 1),
                "latest_whale": state.latest_liquidation,
                "macro": {"fng_value": state.fng_value, "fng_status": state.fng_status},
                "ai_engine": {
                    "direction": direction,
                    "confidence": round(confidence, 1),
                    "score": round(ai_score, 3)
                },
                "nlp_feed": state.reddit_posts
            }
            await websocket.send_json(payload)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
