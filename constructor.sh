#If automatic port-forwards fail or you prefer manual control, run these commands in separate terminals:
docker build -t hospital-monitor:latest .\monitor
docker build -t hospital-admission:latest .\admission
docker build -t hospital-treatment:latest .\treatment
docker build -t hospital-doctor-assignment:latest .\doctor-assignment
docker build -t hospital-discharge:latest .\discharge


kubectl set image deployment/monitor monitor=hospital-monitor:latest -n hospital
kubectl rollout restart deployment/monitor -n hospital
# Port-forward monitor (local 8000 -> cluster monitor:8000)
kubectl port-forward -n hospital svc/monitor 8000:8000

# Port-forward admission (local 8001 -> cluster admission:8000)
kubectl port-forward -n hospital svc/admission 8001:8000

# Port-forward RabbitMQ management UI (local 15672 -> 15672)
kubectl port-forward -n hospital svc/rabbitmq 15672:15672

# Test monitor health locally (after starting port-forward)
curl http://localhost:8000/health

# Simulate arrival (from your machine, requires admission forward to 8001)
curl -X POST http://localhost:8001/simulate_arrival -H "Content-Type: application/json" -d @admission/payloads/juan.json

# Exec into a pod
kubectl get pods -n hospital
kubectl exec -it -n hospital <pod-name> -- bash

# Cleanup
kubectl delete -f k8s.yaml
kubectl get all -n hospital

USAGE

echo "constructor script finished. Automatic port-forwards started in background (check $PIDS_FILE)."
