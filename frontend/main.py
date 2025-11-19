import os
import json
import time
import threading
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, FileResponse
import httpx

SERVICE_NAME = os.getenv("SERVICE_NAME", "frontend")
MONITOR_STREAM = os.getenv("MONITOR_STREAM", "http://monitor:8000/stream")

app = FastAPI(title=SERVICE_NAME)

HERE = os.path.dirname(__file__)

@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


@app.get("/config")
async def config():
    return {"monitor_stream": MONITOR_STREAM}
 
if __name__ == "__main__":
    print("frontend placeholder - monitor-only UI served from static files")
