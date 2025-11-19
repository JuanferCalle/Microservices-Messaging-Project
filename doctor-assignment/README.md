**README — Doctor Assignment Microservice**

Propósito: asignar un médico a un paciente tras triage/triage.completed y emitir `doctor.assigned`.

Puntos claves:
- Consumir `triage.completed`.
- Emitir `doctor.assigned` con `{doctor_id, patient_id}`.
- Implementar `/health`.

See `billing/README.md` for full implementation checklist and AMQP examples.
