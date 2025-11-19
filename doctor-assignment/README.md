# Doctor Assignment Microservice

## Propósito
Asignar automáticamente un médico a un paciente cuando se recibe el evento `triage.completed`.  
Emitir `doctor.assigned` con `{patient_id, doctor_id}`.

## Eventos
### Consume:
- `triage.completed`

### Produce:
- `doctor.assigned`

## Exchange
- Nombre: `hospital`
- Tipo: `topic`

## Puerto
Este microservicio expone su API en el puerto **8003**.

## Endpoints
### GET /health
Responde:
```json
{"status": "ok"}
