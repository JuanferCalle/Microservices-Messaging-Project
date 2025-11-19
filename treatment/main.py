import os
import json
import threading
from fastapi import FastAPI
import pika
import uuid
from datetime import datetime


SERVICE_NAME = os.getenv("SERVICE_NAME", "treatment")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE = os.getenv("TREATMENT_QUEUE", "treatment.q")

app = FastAPI(title=SERVICE_NAME)


# ======================================
# RABBITMQ connection
# ======================================
connection_params = pika.ConnectionParameters(host=RABBIT_HOST)


def publish_message(message: dict, routing_key: str):
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()

    ch.exchange_declare(exchange="hospital", exchange_type="topic", durable=True)
    ch.basic_publish(
        exchange="hospital",
        routing_key=routing_key,
        body=json.dumps(message)
    )

    print(f"[treatment] published routing_key={routing_key} body={message}")
    conn.close()


# ======================================
# CONSUMER
# ======================================

def start_consumer():
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()

    ch.exchange_declare(exchange="hospital", exchange_type="topic", durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)

    # Eventos que este servicio escucha
    bindings = ["doctor.assigned", "triage.completed"]
    for key in bindings:
        ch.queue_bind(exchange="hospital", queue=QUEUE, routing_key=key)
        print(f"[treatment] bound to {key}")

    print("[treatment] consumer ready, waiting for messages...")

    def callback(ch, method, properties, body):
        event = json.loads(body.decode())
        rk = method.routing_key
        print(f"[treatment] received {rk}: {event}")

        # Procesa evento y publica treatment.completed
        outgoing = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "patient_id": event.get("patient_id"),
            "treatment_status": "completed",
            "source": SERVICE_NAME
        }

        publish_message(outgoing, "treatment.completed")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=QUEUE, on_message_callback=callback)
    ch.start_consuming()


# Iniciar el consumer en segundo plano al iniciar FastAPI
@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()
    print("[treatment] consumer thread started")


# ======================================
# ENDPOINTS
# ======================================

@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/")
async def root():
    return {"service": SERVICE_NAME, "message": "Treatment service is running"}


# ======================================
# RUN UVICORN
# ======================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8006)
