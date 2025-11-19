import json
import threading
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI
import pika

# --- Configuración de Entorno (Simulada) ---
# Usamos un exchange de tipo 'topic' para los eventos
RABBITMQ_HOST = "rabbitmq"  # Nombre de host común en entornos Docker Compose
EXCHANGE_NAME = "hospital_events"

# Clave de enrutamiento para CONSUMIR eventos de facturación completados
CONSUME_KEY = "billing.#"  
# Clave de enrutamiento para EMITIR el evento de alta completada
PUBLISH_KEY = "discharge.completed" 

# --- Lógica de Manejo de Mensajes (RabbitMQ) ---

class DischargeService:
    """
    Servicio de Alta (Discharge Service)
    Escucha eventos de facturación y emite el evento 'discharge.completed'.
    """
    def __init__(self, host: str, exchange: str):
        self.host = host
        self.exchange = exchange
        self.connection = None
        self.channel = None
        self.pending_discharges: Dict[str, bool] = {} # Simula el estado de pacientes pendientes de pago

    def connect(self):
        """Intenta conectarse a RabbitMQ con reintentos."""
        max_retries = 10
        for i in range(max_retries):
            try:
                print(f"[{time.time():.2f}] Intentando conectar a RabbitMQ ({i+1}/{max_retries})...")
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(self.host, retry_delay=5, heartbeat=60)
                )
                self.channel = self.connection.channel()
                self.channel.exchange_declare(
                    exchange=self.exchange, 
                    exchange_type='topic', 
                    durable=True
                )
                print(f"[{time.time():.2f}] Conexión a RabbitMQ exitosa.")
                return
            except pika.exceptions.AMQPConnectionError as e:
                print(f"[{time.time():.2f}] Error de conexión: {e}. Reintentando en 5s...")
                time.sleep(5)
        raise ConnectionError("No se pudo conectar a RabbitMQ después de múltiples reintentos.")

    def on_billing_message(self, ch, method, properties, body):
        """
        Callback que se ejecuta cuando se recibe un evento de facturación.
        Funciona para las claves billing.invoice.paid o billing.settled.
        """
        try:
            data = json.loads(body)
            patient_id = data.get("patient_id")
            event_key = method.routing_key
            
            print(f"[{time.time():.2f}] Recibido evento: '{event_key}' para Paciente ID: {patient_id}")
            
            # 1. Confirmar cobros pendientes
            if patient_id and event_key in ["billing.invoice.paid", "billing.settled"]:
                # En un sistema real, aquí buscaríamos el estado de la factura.
                # Simulamos que la recepción de este mensaje indica que todo está pagado.
                
                # Marcar como pagado
                self.pending_discharges[patient_id] = True 
                print(f"[{time.time():.2f}] Cobro confirmado para Paciente ID: {patient_id}. Procediendo con la alta.")
                
                # 2. Emitir 'discharge.completed'
                self.publish_discharge_completed(patient_id, data)
                
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError:
            print(f"[{time.time():.2f}] Error: Mensaje no es un JSON válido: {body}")
            ch.basic_nack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[{time.time():.2f}] Error en el procesamiento del mensaje: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    def consume_billing_events(self):
        """Configura y comienza el consumo de eventos de facturación."""
        try:
            self.connect()

            # Declarar cola exclusiva y temporal (auto_delete=True)
            # o una cola durable con nombre si se necesita persistencia
            # Usar una cola con nombre fijo para el servicio Discharge
            queue_name = "discharge_billing_queue" 
            result = self.channel.queue_declare(queue=queue_name, durable=True)
            
            # Enlazar la cola a las claves de enrutamiento relevantes (billing.#)
            self.channel.queue_bind(
                exchange=self.exchange,
                queue=queue_name,
                routing_key=CONSUME_KEY 
            )

            print(f"[{time.time():.2f}] Esperando eventos en la clave: '{CONSUME_KEY}'")
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=self.on_billing_message
            )
            self.channel.start_consuming()

        except Exception as e:
            print(f"[{time.time():.2f}] Error fatal en el consumidor de RabbitMQ: {e}")
            # Intenta cerrar la conexión de forma segura si existe
            if self.connection and self.connection.is_open:
                self.connection.close()
            # En un entorno de producción, aquí se reiniciaría el servicio.


    def publish_discharge_completed(self, patient_id: str, original_data: Dict[str, Any]):
        """
        Publica el evento 'discharge.completed' a la cola de eventos.
        """
        if not self.channel or not self.connection or not self.connection.is_open:
            print(f"[{time.time():.2f}] Error: Conexión a RabbitMQ no disponible para publicación.")
            return

        message = {
            "patient_id": patient_id,
            "timestamp": time.time(),
            "status": "completed",
            "details": f"Patient {patient_id} has been fully discharged after payment confirmation."
        }
        
        try:
            # Reutilizamos el canal existente (en el hilo de consumo) para publicar
            # Nota: Esto es seguro en este caso porque la publicación es síncrona y ocurre
            # dentro del hilo de consumo, pero en escenarios de alta carga, se
            # preferiría un canal dedicado para publicaciones.
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=PUBLISH_KEY,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer el mensaje persistente
                )
            )
            print(f"[{time.time():.2f}] PUBLICADO: Evento '{PUBLISH_KEY}' para Paciente ID: {patient_id}")
            # Eliminar el estado de paciente pendiente
            self.pending_discharges.pop(patient_id, None)

        except Exception as e:
            print(f"[{time.time():.2f}] ERROR al publicar el evento '{PUBLISH_KEY}': {e}")


# --- Inicialización de FastAPI y Servicio ---

discharge_service = DischargeService(RABBITMQ_HOST, EXCHANGE_NAME)
consumer_thread = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Función de lifespan (FastAPI) para gestionar el hilo del consumidor de RabbitMQ.
    Se ejecuta al iniciar y al detener el servidor.
    """
    global consumer_thread
    
    # --- Al Iniciar ---
    print("Iniciando servicio 'discharge' y consumidor de RabbitMQ...")
    # Creamos y ejecutamos el consumidor en un hilo separado para no bloquear FastAPI
    consumer_thread = threading.Thread(target=discharge_service.consume_billing_events, daemon=True)
    consumer_thread.start()
    
    yield
    
    # --- Al Detener ---
    print("Deteniendo servicio 'discharge' y cerrando conexión de RabbitMQ...")
    if discharge_service.connection and discharge_service.connection.is_open:
        # Cierra la conexión de forma segura, lo que detiene el hilo de consumo
        discharge_service.connection.close()
        consumer_thread.join(timeout=5) # Esperar a que el hilo termine
        print("Conexión de RabbitMQ cerrada y consumidor detenido.")


app = FastAPI(
    title="Servicio Discharge (Alta) - Puerto 8009", # Título actualizado
    description="Procesa la alta de pacientes tras la confirmación de pago.",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
def health_check():
    """Endpoint de verificación de salud."""
    is_rabbitmq_connected = (
        discharge_service.connection is not None and discharge_service.connection.is_open
    )
    return {
        "status": "ok",
        "service": "Discharge (8009)", # Etiqueta actualizada
        "rabbitmq_connected": is_rabbitmq_connected
    }

# Endpoint para simulación (opcional, para testear rápidamente la emisión)
@app.post("/simulate_discharge/{patient_id}")
def simulate_discharge(patient_id: str):
    """
    Simula una alta forzada (en caso de que el cobro ya haya sido procesado o sea manual).
    NOTA: En producción, esto debería ser solo para pagos ya confirmados.
    """
    # En un sistema real, esto requiere una verificación de seguridad y de estado de pago.
    print(f"Simulación de alta forzada para Paciente ID: {patient_id}")
    discharge_service.publish_discharge_completed(patient_id, {})
    return {"message": f"Simulación de evento '{PUBLISH_KEY}' enviada para {patient_id}"}