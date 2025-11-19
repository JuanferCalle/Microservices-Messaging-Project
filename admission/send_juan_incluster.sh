#!/bin/sh
# Create a temporary JSON file and use a curl pod to POST it to the in-cluster admission service

cat > /tmp/juan.json <<'JSON'
{
  "patient_id": "juan-001",
  "name": "Juan",
  "age": 35,
  "symptoms": "dolor de cabeza",
  "arrival_source": "simulado",
  "notes": "Simulación: comprobar que monitor recibe patient.arrived"
}
JSON

echo "Posting /simulate_arrival to in-cluster admission service..."
kubectl run --rm -i --tty curl-sender --image=curlimages/curl -n hospital -- sh -c "curl -sS -X POST http://admission:8000/simulate_arrival -H 'Content-Type: application/json' -d @/tmp/juan.json -w '\nHTTP_CODE:%{http_code}\n'"

echo "Done. Check monitor logs: kubectl logs -n hospital -l app=monitor --tail=200"
