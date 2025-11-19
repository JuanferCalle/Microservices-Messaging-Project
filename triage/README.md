**Super Mega README — Triage Microservice**

Propósito: asignar prioridad tras la llegada del paciente. Consumir `patient.arrived` y emitir `triage.completed` con `{priority: low|medium|high}`.

Implementación:
- Declarar exchange `hospital`, crear cola durable y bind a `patient.arrived`.
- Emitir `triage.completed` con payload mínimo `{patient_id, priority, timestamp}`.
- Proveer `/health`.

Ver `billing/README.md` para detalles técnicos.
