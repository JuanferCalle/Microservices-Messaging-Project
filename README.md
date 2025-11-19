
## Guía rápida de inicio — Para quien acaba de llegar

Este repositorio contiene dos microservicios listos para desarrollo local:

- `admission` — publica eventos `patient.arrived` (FastAPI, puerto interno 8000)
- `monitor` — escucha todos los eventos del exchange `hospital` y expone una UI SSE (FastAPI, puerto interno 8000)

Estructura clave:
- `k8s.yaml` — manifiestos Kubernetes para namespace `hospital` (Deployments + Services).
- `scripts/start-forwards.ps1` y `scripts/stop-forwards.ps1` — scripts para abrir/cerrar `kubectl port-forward` a monitor, admission y RabbitMQ.
- `admission/payloads/juan.json` — payload de ejemplo para probar.

Objetivo de esta guía: dejarte en operación en ~5 minutos.

Requisitos
- `kubectl` configurado y apuntando a tu clúster (Docker Desktop Kubernetes está bien).
- `docker` (si necesitas reconstruir imágenes localmente).

Pasos (rápidos)

1) Aplicar los manifiestos Kubernetes (crea el namespace `hospital` y despliega los pods):

```powershell
kubectl apply -f k8s.yaml
kubectl get pods -n hospital -o wide
```

2) Iniciar los port-forwards (abre 3 ventanas de PowerShell para monitor, admission y RabbitMQ):

```powershell
cd scripts
.\start-forwards.ps1 -MonitorPort 8000 -AdmissionPort 8001 -RabbitPort 15672 -Namespace hospital
```

Esto abrirá nuevas ventanas con `kubectl port-forward`. Si prefieres hacerlo manualmente, usa:

```powershell
# en ventana 1 (monitor)
kubectl port-forward -n hospital svc/monitor 8000:8000
# en ventana 2 (admission)
kubectl port-forward -n hospital svc/admission 8001:8000
# en ventana 3 (rabbitmq management)
kubectl port-forward -n hospital svc/rabbitmq 15672:15672
```

3) Abrir la UI del monitor en el navegador:

```powershell
Start-Process http://localhost:8000
```

4) Acceder al Dashboard de RabbitMQ (management):

```powershell
Start-Process http://localhost:15672
# Usuario/clave por defecto (desarrollo): guest / guest
```

5) Enviar un paciente de prueba (Juan):

Opción A — desde tu máquina (requiere forward a admission en 8001). Ejecuta este comando desde la raíz del repositorio:

```powershell
curl -X POST http://localhost:8001/simulate_arrival -H "Content-Type: application/json" -d @admission/payloads/juan.json
```

Opción B — in-cluster (sin forward):

```powershell
kubectl run --rm -i --tty curl-sender --image=curlimages/curl -n hospital -- sh -c "curl -sS -X POST http://admission:8000/simulate_arrival -H 'Content-Type: application/json' -d @/tmp/juan.json"
```

6) Verificar que el `monitor` recibió el evento:

```powershell
kubectl logs -n hospital -l app=monitor --tail=200
```

Comandos útiles: reconstruir y redeployar `monitor` (si cambias la UI)

```powershell
# reconstruir la imagen localmente
cd hospital-microservices\monitor
docker build -t hospital-monitor:latest .

# actualizar el deployment y forzar rollout
kubectl set image deployment/monitor monitor=hospital-monitor:latest -n hospital
kubectl rollout restart deployment/monitor -n hospital
kubectl rollout status deployment/monitor -n hospital
```

Detener los forwards

```powershell
cd hospital-microservices\scripts
.\stop-forwards.ps1
```

Notas y solución de problemas

- Si la UI sigue mostrando la versión anterior, intenta recargar sin cache (Ctrl+F5) o reconstruir la imagen y redeployar como se indica arriba.
- Si `kubectl port-forward` falla con "address already in use", cierra la sesión que lo usa o elige otro puerto en `start-forwards.ps1`.
- Para desarrollo está bien usar credenciales `guest/guest` en RabbitMQ. En producción, crea usuarios con `rabbitmqctl`.

¿Quieres que aplique los manifiestos y ejecute los forwards ahora para dejarlo listo y luego publique a Juan? Puedo hacerlo y traerte los logs del `monitor`.