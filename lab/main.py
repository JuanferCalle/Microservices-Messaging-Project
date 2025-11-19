import os
"""
lab placeholder

This implementation was removed as part of workspace cleanup. If you need to reimplement
the `lab` service, create a FastAPI app that connects to RabbitMQ and processes
messages bound to `CONSUME_KEY`, then publishes to `PRODUCE_KEY`.
"""

print("lab placeholder - implementation removed")
CONSUME_KEY = os.getenv("CONSUME_KEY", "doctor.assigned")
