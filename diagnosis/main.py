import os
import json
import threading
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
import pika
import uuid

SERVICE_NAME = os.getenv("SERVICE_NAME", "diagnosis")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
CONSUME_KEY = os.getenv("CONSUME_KEY", "exam.completed")

app = FastAPI(title=SERVICE_NAME)

connection_params = pika.ConnectionParameters(host=RABBIT_HOST)

# Track processed messages for idempotency
processed_messages = set()

# In-memory diagnosis storage
diagnoses = {}


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


def process_exam_completed(message_data: dict):
    """Process exam.completed event and produce diagnosis events"""
    patient_id = message_data.get("patient_id")
    event_id = message_data.get("event_id", str(uuid.uuid4()))
    
    # Check for idempotency
    if event_id in processed_messages:
        print(f"[SKIP] Duplicate message event_id={event_id}")
        return
    
    processed_messages.add(event_id)
    print(f"[PROCESS] exam.completed for patient_id={patient_id}")
    
    # Get exam result from payload
    payload = message_data.get("payload", {})
    exam_result = payload.get("result", "normal")
    exam_type = payload.get("exam_type", "unknown")
    
    # Initialize diagnosis record if not exists
    if patient_id not in diagnoses:
        diagnoses[patient_id] = {
            "patient_id": patient_id,
            "exams": [],
            "status": "pending"
        }
    
    # Add exam to diagnosis record
    diagnoses[patient_id]["exams"].append({
        "exam_type": exam_type,
        "result": exam_result,
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    })
    
    # Decide next action based on exam result
    if exam_result == "abnormal" or exam_result == "inconclusive":
        # Request additional diagnosis/exams
        diagnosis_request = {
            "patient_id": patient_id,
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload": {
                "reason": f"{exam_result} exam results require further investigation",
                "previous_exam": exam_type,
                "previous_result": exam_result,
                "requested_by": SERVICE_NAME,
                "original_event_id": event_id
            }
        }
        diagnoses[patient_id]["status"] = "additional_exams_requested"
        publish_message(diagnosis_request, "diagnosis.requested")
        print(f"[ACTION] Requested additional diagnosis for patient_id={patient_id} due to {exam_result} results")
    else:
        # Complete diagnosis
        diagnosis_complete = {
            "patient_id": patient_id,
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload": {
                "diagnosis": "Patient is healthy based on exam results",
                "exam_count": len(diagnoses[patient_id]["exams"]),
                "final_result": "normal",
                "completed_by": SERVICE_NAME,
                "original_event_id": event_id
            }
        }
        diagnoses[patient_id]["status"] = "completed"
        publish_message(diagnosis_complete, "diagnosis.completed")
        print(f"[ACTION] Diagnosis completed for patient_id={patient_id}")


def consume_messages():
    """Consumer thread that listens to exam.completed events"""
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
            process_exam_completed(message_data)
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
        "message": "Diagnosis service is running",
        "consumes": CONSUME_KEY,
        "produces": ["diagnosis.completed", "diagnosis.requested"]
    }


@app.get("/diagnoses")
async def get_diagnoses():
    """Get all diagnoses"""
    return {"diagnoses": diagnoses}


@app.get("/diagnoses/{patient_id}")
async def get_diagnosis(patient_id: str):
    """Get diagnosis for a specific patient"""
    if patient_id not in diagnoses:
        return {"error": "Diagnosis not found", "patient_id": patient_id}
    return {"diagnosis": diagnoses[patient_id]}


@app.post("/simulate_exam_completed")
async def simulate_exam_completed(payload: dict, background: BackgroundTasks):
    """Simulate receiving an exam.completed event"""
    if "patient_id" not in payload:
        return {"error": "patient_id is required"}
    
    message = {
        "patient_id": payload["patient_id"],
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": payload.get("payload", {"exam_type": "blood_test", "result": "normal"})
    }
    
    background.add_task(publish_message, message, "exam.completed")
    return {
        "result": "published",
        "routing_key": "exam.completed",
        "patient_id": payload["patient_id"]
    }

