import os
"""
billing placeholder

This implementation was removed as part of workspace cleanup. If you need to reimplement
the `billing` service, create a FastAPI app that connects to RabbitMQ and processes
messages bound to `CONSUME_KEY`, then publishes to `PRODUCE_KEY`.
"""

print("billing placeholder - implementation removed")
CONSUME_KEY = os.getenv("CONSUME_KEY", "pharmacy.completed")
