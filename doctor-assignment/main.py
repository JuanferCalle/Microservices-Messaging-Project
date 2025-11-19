import os
import json
import threading
import time
import uuid
from datetime import datetime

import pika
from fastapi import FastAPI

SERVICE_NAME = os.getenv("SERVICE_NAME", "doctor-assignment")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

app = FastAPI(title=SERVICE_NAME)

connection_params = pika.ConnectionParameters(host=RABBIT_HOST)

# Lista cíclica simple de doctores
DOCTORS = ["doc-1", "doc-2", "doc-3"]
doctor_index = 0


def pick_doctor():
    global doctor_index
    doctor_id = DOCTORS[doctor_index % len(DOCTORS)]
    doctor_index += 1
    return doctor_id


def publish_event(event: dict, routing_key: str):
    """Publica un evento en el exchange hospital."""
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()

    ch.exchange_declare(exchange="hospital", exchange_type="topic", durable=True)
    ch.basic_publish(
        exchange="hospital",
        routing_key=routing_key,
        body=json.dumps(event)
    )

    print(f"[doctor-assignment] Published {routing_key}: {event}")
    conn.close()


def process_message(ch, method, properties, body):
    """Callback cuando llega triage.completed"""

    try:
        data = json.loads(body)
    except Exception:
        print("❌ Received invalid JSON")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    patient_id = data.get("patient_id")
    if not patient_id:
        print("❌ triage.completed missing patient_id")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    doctor_id = pick_doctor()

    event = {
        "patient_id": patient_id,
        "doctor_id": doctor_id,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

    publish_event(event, "doctor.assigned")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    """Thread que queda escuchando triage.completed"""
    print("👂 doctor-assignment waiting for triage.completed...")

    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()

    ch.exchange_declare(exchange="hospital", exchange_type="topic", durable=True)

    # Cola duradera y bind
    queue_name = "doctor-assignment-triage-completed"
    ch.queue_declare(queue=queue_name, durable=True)
    ch.queue_bind(
        exchange="hospital",
        queue=queue_name,
        routing_key="triage.completed"
    )

    ch.basic_consume(queue=queue_name, on_message_callback=process_message)

    ch.start_consuming()


@app.on_event("startup")
def startup_event():
    """Arranca un thread separado para consumir mensajes"""
    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}
