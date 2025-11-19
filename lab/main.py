import os
import json
import time
import threading
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
import pika
import uuid

SERVICE_NAME = os.getenv("SERVICE_NAME", "lab")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
CONSUME_KEY = os.getenv("CONSUME_KEY", "doctor.assigned")

app = FastAPI(title=SERVICE_NAME)

connection_params = pika.ConnectionParameters(host=RABBIT_HOST)

# Track processed messages for idempotency
processed_messages = set()


def publish_message(message: dict, routing_key: str):
    """Publish a message to the hospital exchange"""
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()
    ch.exchange_declare(exchange='hospital', exchange_type='topic', durable=True)
    ch.basic_publish(
        exchange='hospital',
        routing_key=routing_key,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
    )
    pid = message.get("patient_id", "unknown")
    print(f"[PUBLISH] routing_key={routing_key} patient_id={pid}")
    conn.close()


def process_doctor_assigned(message_data: dict):
    """Process doctor.assigned event and produce exam events"""
    patient_id = message_data.get("patient_id")
    event_id = message_data.get("event_id", str(uuid.uuid4()))
    
    # Check for idempotency
    if event_id in processed_messages:
        print(f"[SKIP] Duplicate message event_id={event_id}")
        return
    
    processed_messages.add(event_id)
    print(f"[PROCESS] doctor.assigned for patient_id={patient_id}")
    
    # Simulate exam request and completion
    exam_request = {
        "patient_id": patient_id,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": {
            "exam_type": "blood_test",
            "requested_by": SERVICE_NAME,
            "original_event_id": event_id
        }
    }
    publish_message(exam_request, "exam.requested")
    print(f"[ACTION] Exam requested for patient_id={patient_id}")
    
    # Simulate exam completion (in real scenario this would be async/delayed)
    time.sleep(0.1)
    exam_complete = {
        "patient_id": patient_id,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": {
            "exam_type": "blood_test",
            "result": "normal",
            "completed_by": SERVICE_NAME,
            "original_event_id": event_id
        }
    }
    publish_message(exam_complete, "exam.completed")
    print(f"[ACTION] Exam completed for patient_id={patient_id}")


def consume_messages():
    """Consumer thread that listens to doctor.assigned events"""
    print(f"[CONSUMER] Starting consumer for routing_key={CONSUME_KEY}")
    
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    
    # Declare exchange
    channel.exchange_declare(exchange='hospital', exchange_type='topic', durable=True)
    
    # Declare a durable queue for this service
    queue_name = f"{SERVICE_NAME}_queue"
    channel.queue_declare(queue=queue_name, durable=True)
    
    # Bind queue to routing key
    channel.queue_bind(exchange='hospital', queue=queue_name, routing_key=CONSUME_KEY)
    
    print(f"[CONSUMER] Bound queue={queue_name} to routing_key={CONSUME_KEY}")
    
    def callback(ch, method, properties, body):
        try:
            message_data = json.loads(body)
            print(f"[RECEIVE] {method.routing_key} - {body.decode()}")
            process_doctor_assigned(message_data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Invalid JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            print(f"[ERROR] Processing failed: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    try:
        print(f"[CONSUMER] Waiting for messages...")
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


@app.on_event("startup")
async def startup_event():
    """Start the consumer thread on application startup"""
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    print(f"[STARTUP] {SERVICE_NAME} service started")


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/")
async def root():
    return {
        "service": SERVICE_NAME,
        "message": "Lab service is running",
        "consumes": CONSUME_KEY,
        "produces": ["exam.requested", "exam.completed"]
    }


@app.post("/simulate_doctor_assigned")
async def simulate_doctor_assigned(payload: dict, background: BackgroundTasks):
    """Simulate receiving a doctor.assigned event"""
    if "patient_id" not in payload:
        return {"error": "patient_id is required"}
    
    message = {
        "patient_id": payload["patient_id"],
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": payload.get("payload", {"doctor": "Dr. Smith"})
    }
    
    background.add_task(publish_message, message, "doctor.assigned")
    return {
        "result": "published",
        "routing_key": "doctor.assigned",
        "patient_id": payload["patient_id"]
    }
