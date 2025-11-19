
import os
import json
import time
import threading
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
import pika

SERVICE_NAME = os.getenv("SERVICE_NAME", "triage")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")


app = FastAPI(title=SERVICE_NAME)

connection_params = pika.ConnectionParameters(host=RABBIT_HOST)


def publish_message(message: dict, routing_key: str = "triage.completed"):
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()
    ch.exchange_declare(exchange='hospital', exchange_type='topic', durable=True)
    ch.basic_publish(exchange='hospital', routing_key=routing_key, body=json.dumps(message))
    # Log for observability: who we published and to which routing key
    try:
        pid = message.get("patient_id") if isinstance(message, dict) else None
    except Exception:
        pid = None
    print(f"Triage published routing_key={routing_key} patient_id={pid}")
    conn.close()


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/")
async def root():
    return {"service": SERVICE_NAME, "message": "Triage service is running"}


CONSUME_KEY = os.getenv("CONSUME_KEY", "patient.arrived")
PRODUCE_KEY = os.getenv("PRODUCE_KEY", "triage.completed")


def process_triage(patient_data: dict) -> dict:
    """Añade información de triage al paciente"""
    patient_id = patient_data.get("patient_id")
    # Asignar prioridad basada en patient_id (simulación simple)
    if patient_id and patient_id % 3 == 0:
        priority = "high"
    elif patient_id and patient_id % 2 == 0:
        priority = "medium"
    else:
        priority = "low"
    
    patient_data["triage_priority"] = priority
    patient_data["triage_timestamp"] = datetime.utcnow().isoformat()
    print(f"Triage: patient_id={patient_id} assigned priority={priority}")
    return patient_data


def start_consumer():
    """Consumer que escucha mensajes de RabbitMQ"""
    def callback(ch, method, properties, body):
        try:
            patient_data = json.loads(body)
            patient_id = patient_data.get("patient_id")
            print(f"Triage: received patient_id={patient_id} from {CONSUME_KEY}")
            
            # Procesar triage
            processed_data = process_triage(patient_data)
            
            # Publicar resultado
            publish_message(processed_data, PRODUCE_KEY)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Triage: error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    try:
        conn = pika.BlockingConnection(connection_params)
        ch = conn.channel()
        ch.exchange_declare(exchange='hospital', exchange_type='topic', durable=True)
        
        # Cola exclusiva
        result = ch.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        
        ch.queue_bind(exchange='hospital', queue=queue_name, routing_key=CONSUME_KEY)
        print(f"Triage: consumer started, listening to {CONSUME_KEY}")
        
        ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        ch.start_consuming()
    except Exception as e:
        print(f"Triage: consumer error: {e}")


@app.on_event("startup")
async def startup_event():
    """Inicia el consumer en background al arrancar FastAPI"""
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()
    print("Triage: consumer thread started")