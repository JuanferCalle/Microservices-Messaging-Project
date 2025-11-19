Scripts para port-forward

start-forwards.ps1
- Inicia 3 ventanas de PowerShell con `kubectl port-forward` para monitor, admission y rabbitmq.
- Guarda los PIDs en `forward-pids.json`.

stop-forwards.ps1
- Detiene las ventanas/ procesos listados en `forward-pids.json` y borra el archivo.

Ejemplo de uso (PowerShell):

1) Abre una terminal en la carpeta `hospital-microservices/scripts` y arranca:

```powershell
.\start-forwards.ps1 -MonitorPort 8000 -AdmissionPort 8001 -RabbitPort 15672 -Namespace hospital
```

2) Abre el navegador: `http://localhost:8000` (monitor) o `http://localhost:8001` (admission)

3) Cuando termines, detén los forwards:

```powershell
.\stop-forwards.ps1
```

Notas:
- Estos scripts abren nuevas ventanas de PowerShell para que puedas ver la salida de `kubectl port-forward`.
- Si prefieres que se ejecuten en background sin nuevas ventanas, podemos cambiar la implementación a `Start-Job`.
