import os
import json
import threading
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
import pika
import uuid

SERVICE_NAME = os.getenv("SERVICE_NAME", "billing")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

app = FastAPI(title=SERVICE_NAME)

connection_params = pika.ConnectionParameters(host=RABBIT_HOST)

# Track processed messages for idempotency
processed_messages = set()

# In-memory invoice storage (accumulate charges per patient)
invoices = {}


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


def process_treatment_completed(message_data: dict):
    """Process treatment.completed event and generate provisional invoice"""
    patient_id = message_data.get("patient_id")
    event_id = message_data.get("event_id", str(uuid.uuid4()))
    
    # Check for idempotency
    if event_id in processed_messages:
        print(f"[SKIP] Duplicate message event_id={event_id}")
        return
    
    processed_messages.add(event_id)
    print(f"[PROCESS] treatment.completed for patient_id={patient_id}")
    
    # Initialize invoice if not exists
    if patient_id not in invoices:
        invoices[patient_id] = {
            "patient_id": patient_id,
            "charges": [],
            "total": 0.0
        }
    
    # Add treatment charge
    treatment_cost = 150.0  # Base treatment cost
    invoices[patient_id]["charges"].append({
        "type": "treatment",
        "description": "Medical treatment",
        "amount": treatment_cost,
        "event_id": event_id
    })
    invoices[patient_id]["total"] += treatment_cost
    
    print(f"[ACTION] Added treatment charge ${treatment_cost} for patient_id={patient_id}")
    
    # Emit invoice created event
    invoice_event = {
        "patient_id": patient_id,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": {
            "invoice": invoices[patient_id],
            "status": "provisional",
            "created_by": SERVICE_NAME,
            "original_event_id": event_id
        }
    }
    publish_message(invoice_event, "billing.invoice.created")
    print(f"[ACTION] Invoice created for patient_id={patient_id}, total=${invoices[patient_id]['total']}")


def process_pharmacy_dispensed(message_data: dict):
    """Process pharmacy.dispensed event and add medication cost to invoice"""
    patient_id = message_data.get("patient_id")
    event_id = message_data.get("event_id", str(uuid.uuid4()))
    
    # Check for idempotency
    if event_id in processed_messages:
        print(f"[SKIP] Duplicate message event_id={event_id}")
        return
    
    processed_messages.add(event_id)
    print(f"[PROCESS] pharmacy.dispensed for patient_id={patient_id}")
    
    # Initialize invoice if not exists
    if patient_id not in invoices:
        invoices[patient_id] = {
            "patient_id": patient_id,
            "charges": [],
            "total": 0.0
        }
    
    # Add medication charge
    medication_cost = message_data.get("payload", {}).get("cost", 50.0)
    medication_name = message_data.get("payload", {}).get("medication", "Generic medication")
    
    invoices[patient_id]["charges"].append({
        "type": "medication",
        "description": medication_name,
        "amount": medication_cost,
        "event_id": event_id
    })
    invoices[patient_id]["total"] += medication_cost
    
    print(f"[ACTION] Added medication charge ${medication_cost} for patient_id={patient_id}")
    
    # Emit updated invoice event
    invoice_event = {
        "patient_id": patient_id,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": {
            "invoice": invoices[patient_id],
            "status": "updated",
            "created_by": SERVICE_NAME,
            "original_event_id": event_id
        }
    }
    publish_message(invoice_event, "billing.invoice.created")
    print(f"[ACTION] Invoice updated for patient_id={patient_id}, total=${invoices[patient_id]['total']}")


def consume_messages():
    """Consumer thread that listens to treatment and pharmacy events"""
    print(f"[CONSUMER] Starting consumer for treatment.completed and pharmacy.dispensed")
    
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    
    # Declare exchange
    channel.exchange_declare(exchange='hospital', exchange_type='topic', durable=True)
    
    # Declare a durable queue for this service
    queue_name = f"{SERVICE_NAME}_queue"
    channel.queue_declare(queue=queue_name, durable=True)
    
    # Bind queue to multiple routing keys
    channel.queue_bind(exchange='hospital', queue=queue_name, routing_key="treatment.completed")
    channel.queue_bind(exchange='hospital', queue=queue_name, routing_key="pharmacy.dispensed")
    
    print(f"[CONSUMER] Bound queue={queue_name} to routing_keys=[treatment.completed, pharmacy.dispensed]")
    
    def callback(ch, method, properties, body):
        try:
            message_data = json.loads(body)
            print(f"[RECEIVE] {method.routing_key} - {body.decode()}")
            
            # Route to appropriate handler based on routing key
            if method.routing_key == "treatment.completed":
                process_treatment_completed(message_data)
            elif method.routing_key == "pharmacy.dispensed":
                process_pharmacy_dispensed(message_data)
            
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
        "message": "Billing service is running",
        "consumes": ["treatment.completed", "pharmacy.dispensed"],
        "produces": ["billing.invoice.created"]
    }


@app.get("/invoices")
async def get_invoices():
    """Get all invoices"""
    return {"invoices": invoices}


@app.get("/invoices/{patient_id}")
async def get_invoice(patient_id: str):
    """Get invoice for a specific patient"""
    if patient_id not in invoices:
        return {"error": "Invoice not found", "patient_id": patient_id}
    return {"invoice": invoices[patient_id]}


@app.post("/simulate_treatment_completed")
async def simulate_treatment_completed(payload: dict, background: BackgroundTasks):
    """Simulate receiving a treatment.completed event"""
    if "patient_id" not in payload:
        return {"error": "patient_id is required"}
    
    message = {
        "patient_id": payload["patient_id"],
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": payload.get("payload", {"treatment": "Standard treatment"})
    }
    
    background.add_task(publish_message, message, "treatment.completed")
    return {
        "result": "published",
        "routing_key": "treatment.completed",
        "patient_id": payload["patient_id"]
    }


@app.post("/simulate_pharmacy_dispensed")
async def simulate_pharmacy_dispensed(payload: dict, background: BackgroundTasks):
    """Simulate receiving a pharmacy.dispensed event"""
    if "patient_id" not in payload:
        return {"error": "patient_id is required"}
    
    message = {
        "patient_id": payload["patient_id"],
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": payload.get("payload", {"medication": "Generic medication", "cost": 50.0})
    }
    
    background.add_task(publish_message, message, "pharmacy.dispensed")
    return {
        "result": "published",
        "routing_key": "pharmacy.dispensed",
        "patient_id": payload["patient_id"]
    }
