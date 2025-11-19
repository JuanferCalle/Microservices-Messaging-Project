**Super Mega README — Discharge Microservice**

Resumen: cuando el paciente está listo para irse, este servicio procesa la salida, confirma cobros pendientes y emite `discharge.completed`.

Funciones clave:
- Consumir `billing.invoice.paid` o `billing.settled` antes de permitir `discharge.completed`.
- Emitir `discharge.completed` cuando todo listo.

Implementación: seguir checklist de `billing/README.md`.
