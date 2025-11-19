**Super Mega README — Frontend (Monitor) Placeholder**

Nota: este repositorio usa `monitor` como UI. Si vas a crear un frontend propio, sigue estas indicaciones:

- Servir archivos estáticos (HTML/JS/CSS) en `/` y conectar a `GET /stream` del monitor (SSE) para recibir eventos.
- No publicar eventos directamente a RabbitMQ desde el navegador. Usa un backend para seguridad.
- Añadir un `Dockerfile` que sirva los archivos con `nginx` o un simple servidor estático.

Checklist mínima:
- `index.html` que abra `EventSource('/stream')`.
- Manejo de reconexión y backoff.
- Mapeo de routing keys a columnas de la UI.
