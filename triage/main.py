"""
triage placeholder

This implementation was removed as part of workspace cleanup. If you need to reimplement
the `triage` service, create a FastAPI app that:

- Connects to RabbitMQ at `RABBITMQ_HOST` (env var)
- Declares the `hospital` topic exchange
- Binds an exclusive queue to the `CONSUME_KEY` routing key
- Processes messages and publishes to `PRODUCE_KEY`

This file is intentionally a placeholder.
"""

print("triage placeholder - implementation removed")
