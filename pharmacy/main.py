import os
"""
pharmacy placeholder

This implementation was removed as part of workspace cleanup. If you need to reimplement
the `pharmacy` service, create a FastAPI app that connects to RabbitMQ and processes
messages bound to `CONSUME_KEY`, then publishes to `PRODUCE_KEY`.
"""

print("pharmacy placeholder - implementation removed")
CONSUME_KEY = os.getenv("CONSUME_KEY", "treatment.completed")
