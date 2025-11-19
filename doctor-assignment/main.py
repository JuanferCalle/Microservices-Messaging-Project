import asyncio
import json
import os
import uuid
from datetime import datetime

from fastapi import FastAPI
from aio_pika import connect_robust, Message, ExchangeType, IncomingMessage

app = FastAPI(title="doctor-assignment")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/")
EXCHANGE_NAME = "hospital"

ROUTING_KEY_IN = "triage.completed"
ROUTING_KEY_OUT = "doctor.assigned"
QUEUE_NAME = "doctor-assignment-triage-completed"

connection = None
channel = None
exchange = None

# Lista simple de doctores
DOCTORS = ["doc-1", "doc-2", "doc-3"]
current_doctor = 0
doctor_lock = asyncio.Lock()


async def choose_doctor():
    global current_doctor
    async with doctor_lock:
        doctor = DOCTORS[current_doctor % len(DOCTORS)]
        current_doctor += 1
        return doctor


async def process_triage(message: IncomingMessage):
    async with message.process():
        try:
            data = json.loads(message.body)
        except:
            print("❌ Mensaje inválido, no es JSON")
            return

        patient_id = data.get("patient_id")
        if not patient_id:
            print("❌ triage.completed sin patient_id")
            return

        doctor_id = await choose_doctor()

        event = {
            "patient_id": patient_id,
            "doctor_id": doctor_id,
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        body = json.dumps(event).encode()

        await exchange.publish(
            Message(body=body, content_type="application/json"),
            routing_key=ROUTING_KEY_OUT
        )

        print(f"✔ doctor.assigned enviado → {event}")


async def setup_rabbitmq():
    global connection, channel, exchange
    connection = await connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        ExchangeType.TOPIC,
        durable=True
    )

    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key=ROUTING_KEY_IN)
    await queue.consume(process_triage)

    print("👂 doctor-assignment escuchando triage.completed...")


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(setup_rabbitmq())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.on_event("shutdown")
async def shutdown_event():
    if connection:
        await connection.close()

