**README — Billing Microservice**

- **Propósito:** Implementar facturación para el paciente (recibe eventos de finalización de tratamiento/medicación y genera factura). Debe integrarse con el exchange `hospital`.

- **Exchange y routing keys**
  - Usar exchange `hospital` de tipo `topic`.
  - Eventos relevantes que puede consumir/emitir:
    - `treatment.completed` (consumir) — generar factura provisional
    - `pharmacy.dispensed` (consumir) — añadir coste de medicación
    - `billing.invoice.created` (emitir) — notificar que la factura fue creada

- **Requisitos mínimos del servicio**
  - Endpoint HTTP `/health` que devuelva 200 JSON `{"status":"ok"}`.
  - Listener AMQP que declare la exchange `hospital` y su propia cola (durable) enlazada a las routing keys necesarias.
  - Al recibir mensajes: validar JSON, registrar (log) la recepción, procesar, emitir eventos resultantes si aplica y hacer `basic_ack`.
  - Idempotencia: usar `message_id` o `patient_id` + `event_timestamp` para evitar procesar mensajes duplicados.

- **Mensajería (esquema sugerido)**
  - Mensaje básico:
    {
      "patient_id": "juan-001",
      "event_id": "uuid-v4",
      "timestamp": "2025-11-19T12:34:56Z",
      "payload": { ... }
    }

- **Dockerfile (sugerencia rápida)**
  - Base: `python:3.11-slim`
  - Instalar `pika`, `fastapi`, `uvicorn` y dependencias de tu servicio
  - Copiar `main.py` y `requirements.txt`
  - CMD: `uvicorn main:app --host 0.0.0.0 --port 8000`

- **Kubernetes**
  - Añadir Deployment y Service con etiquetas `app: billing`.
  - Asegurar `imagePullPolicy: IfNotPresent` para imágenes locales en Docker Desktop.

- **Logging y observabilidad**
  - Registrar cuando se recibe un evento y cuando se envía uno nuevo.
  - Emitir métricas básicas (mensajes recibidos, fallos) si es posible.

- **Pruebas**
  - Prueba unitaria para la función que transforma eventos en cargos.
  - Prueba de integración: script que publica `treatment.completed` y comprueba `kubectl logs` o que la API `/health` esté arriba.

---

Usa este README como plantilla para implementar el microservicio de billing. Incluye ejemplos de código y pruebas en el repositorio cuando lo implementes.
