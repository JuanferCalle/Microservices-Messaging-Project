# Payloads and helper scripts for `admission`

Esta carpeta tiene payloads de ejemplo y scripts para facilitar enviar un `patient.arrived` al servicio `admission`.

Archivos:

- `payloads/juan.json` — payload JSON para el paciente Juan.
- `send_juan_local.ps1` — script PowerShell para enviar `juan.json` a `http://localhost:8001/simulate_arrival`. Requiere que hagas:

  ```powershell
  kubectl port-forward -n hospital svc/admission 8001:8000
  ```

  y luego ejecutar este script en otra terminal PowerShell:

  ```powershell
  .\send_juan_local.ps1
  ```

- `send_juan_incluster.sh` — script que crea un archivo temporal y lanza un pod `curl` en el namespace `hospital` para POSTear directamente a `http://admission:8000/simulate_arrival`.

Uso recomendado:

1. Si quieres ver la UI del `monitor`, en una terminal nueva ejecuta:

   ```powershell
   kubectl port-forward -n hospital svc/monitor 8000:8000
   Start-Process http://localhost:8000
   ```

2. En otra terminal PowerShell, abre el port-forward de `admission` y ejecuta el script local:

   ```powershell
   kubectl port-forward -n hospital svc/admission 8001:8000
   .\send_juan_local.ps1
   ```

3. Alternativamente puedes ejecutar el script `send_juan_incluster.sh` desde tu máquina (necesitarás `kubectl`):

   ```sh
   sh send_juan_incluster.sh
   ```

Comprobación:

- Logs del monitor:

  ```sh
  kubectl logs -n hospital -l app=monitor --tail=200
  ```

- Debes ver una entrada que indique que el `monitor` recibió un evento con `routing_key: patient.arrived` y el payload de `juan`.
