import os
"""
diagnosis placeholder

This implementation was removed as part of workspace cleanup. If you need to reimplement
the `diagnosis` service, create a FastAPI app that connects to RabbitMQ and processes
messages bound to `CONSUME_KEY`, then publishes to `PRODUCE_KEY`.
"""

print("diagnosis placeholder - implementation removed")
CONSUME_KEY = os.getenv("CONSUME_KEY", "exam.completed")
