from fastapi import FastAPI
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import os
import json
import time
import threading
import pika

SERVICE_NAME = os.getenv("SERVICE_NAME", "monitor")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
CONSUME_KEY = os.getenv("CONSUME_KEY", "#")

app = FastAPI(title=SERVICE_NAME)
connection_params = pika.ConnectionParameters(host=RABBIT_HOST)

# in-memory buffer for events
events = []


def publish_to_buffer(event: dict):
    events.append(event)
    if len(events) > 1000:
        events.pop(0)


def on_message(ch, method, properties, body):
    try:
        payload = json.loads(body)
    except Exception:
        payload = {"raw": body.decode(errors='ignore')}
    event = {"routing_key": method.routing_key, "payload": payload, "timestamp": time.time()}
    print("Monitor received", event)
    publish_to_buffer(event)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    try:
        conn = pika.BlockingConnection(connection_params)
        ch = conn.channel()
        ch.exchange_declare(exchange='hospital', exchange_type='topic', durable=True)
        q = ch.queue_declare(queue='', exclusive=True)
        queue_name = q.method.queue
        ch.queue_bind(exchange='hospital', queue=queue_name, routing_key=CONSUME_KEY)
        ch.basic_consume(queue=queue_name, on_message_callback=on_message)
        print(f"{SERVICE_NAME} consumer started, listening to all topics...")
        ch.start_consuming()
    except Exception as e:
        print("Monitor consumer error:", e)
        time.sleep(3)
        start_consumer()


def event_stream():
    last_index = 0
    while True:
        while last_index < len(events):
            ev = events[last_index]
            last_index += 1
            yield f"data: {json.dumps(ev)}\n\n"
        time.sleep(0.5)


@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()

# serve static UI
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def ui_index():
    idx = os.path.join(static_dir, "index.html")
    if os.path.exists(idx):
        return FileResponse(idx)
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/stream")
async def stream():
    return StreamingResponse(event_stream(), media_type='text/event-stream')
