import os
import json
import time
from fastapi import FastAPI, BackgroundTasks
import pika

SERVICE_NAME = os.getenv("SERVICE_NAME", "hospitalize")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

app = FastAPI(title=SERVICE_NAME)

connection_params = pika.ConnectionParameters(host=RABBIT_HOST)


def publish_message(message: dict, routing_key: str = "patient.arrived"):
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()
    ch.exchange_declare(exchange='hospital', exchange_type='topic', durable=True)
    ch.basic_publish(exchange='hospital', routing_key=routing_key, body=json.dumps(message))
    # Log for observability: who we published and to which routing key
    try:
        pid = message.get("patient_id") if isinstance(message, dict) else None
    except Exception:
        pid = None
    print(f"hospitalize published routing_key={routing_key} patient_id={pid}")
    conn.close()


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/")
async def root():
    return {"service": SERVICE_NAME, "message": "hospitalize service is running"}


# Pre-seeded patients for hospitalize
PRESEED = [
    {"patient_id": 1, "name": "Alice"},
    {"patient_id": 2, "name": "Bob"},
    {"patient_id": 3, "name": "Carlos"},
    {"patient_id": 4, "name": "Diana"},
    {"patient_id": 5, "name": "Eva"},
    {"patient_id": 6, "name": "Fernando"},
    {"patient_id": 7, "name": "Gabriela"},
    {"patient_id": 8, "name": "Hector"}
]


@app.post("/seed")
async def seed(background: BackgroundTasks):
    def do_seed():
        for p in PRESEED:
            try:
                publish_message(p, "patient.arrived")
                print(f"Seed: published patient.arrived for {p}")
                time.sleep(0.1)
            except Exception as e:
                print("publish error:", e)

    background.add_task(do_seed)
    return {"result": "seeding", "count": len(PRESEED)}


@app.post("/simulate_hospitalize")
async def simulate_hospitalize(payload: dict, background: BackgroundTasks):
    # Publish single hospitalize event
    background.add_task(publish_message, payload, "patient.arrived")
    try:
        pid = payload.get("patient_id") if isinstance(payload, dict) else None
    except Exception:
        pid = None
    print(f"simulate_hospitalize: queued publish patient.arrived for patient_id={pid}")
    return {"result": "published", "routing_key": "patient.arrived", "patient_id": pid}
