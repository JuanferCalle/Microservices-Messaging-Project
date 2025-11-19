#!/usr/bin/env bash
set -euo pipefail

# constructor.sh - build images, apply k8s manifests and open port-forwards (bash/WSL/Git-Bash)
# Usage: ./constructor.sh

ROOT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
NAMESPACE=${NAMESPACE:-hospital}

echo "Root dir: $ROOT_DIR"

command -v docker >/dev/null 2>&1 || { echo "docker not found in PATH" >&2; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found in PATH" >&2; exit 1; }

echo "Building Docker images for subdirectories with a Dockerfile..."
built=0
for d in "$ROOT_DIR"/*/ ; do
	[ -d "$d" ] || continue
	if [ -f "$d/Dockerfile" ]; then
		svc=$(basename "$d")
		img="hospital-${svc}:latest"
		echo "- Building $img from $d"
		docker build -t "$img" "$d"
		built=$((built+1))
	fi
done

if [ $built -eq 0 ]; then
	echo "No Dockerfiles found - nothing was built." >&2
else
	echo "Built $built images."
fi

echo "Applying Kubernetes manifests: k8s.yaml"
kubectl apply -f "$ROOT_DIR/k8s.yaml"

echo "Waiting for pods to initialize..."
sleep 3

echo "Pods in namespace $NAMESPACE:"
kubectl get pods -n "$NAMESPACE" -o wide || kubectl get pods -o wide

echo "Services in namespace $NAMESPACE:"
kubectl get svc -n "$NAMESPACE" || kubectl get svc

# Start port-forwards in background and save PIDs + mapping
mkdir -p "$ROOT_DIR/scripts/logs"
PIDS_FILE="$ROOT_DIR/scripts/forward-pids.json"
LOG_DIR="$ROOT_DIR/scripts/logs"

echo "Starting kubectl port-forwards in background (monitor, admission, rabbitmq)..."
forwards=()

start_forward() {
	local svc="$1"; local localport="$2"; local svcport="$3"; local logfile="$4"
	nohup kubectl port-forward -n "$NAMESPACE" svc/$svc ${localport}:${svcport} >"$logfile" 2>&1 &
	pid=$!
	echo "Started forward svc/$svc -> localhost:${localport} (pid $pid), log: $logfile"
	forwards+=("$svc,$localport,$svcport,$pid")
}

start_forward monitor 8000 8000 "$LOG_DIR/monitor-forward.log"
start_forward admission 8001 8000 "$LOG_DIR/admission-forward.log"
start_forward rabbitmq 15672 15672 "$LOG_DIR/rabbitmq-forward.log"

# write mapping JSON
echo -n "{" > "$PIDS_FILE"
echo -n "\"namespace\": \"$NAMESPACE\"," >> "$PIDS_FILE"
echo -n "\"forwards\": [" >> "$PIDS_FILE"
first=true
for f in "${forwards[@]}"; do
	IFS=',' read -r svc lp sp pid <<< "$f"
	if [ "$first" = true ]; then
		first=false
	else
		echo -n "," >> "$PIDS_FILE"
	fi
	echo -n "{\"service\": \"$svc\", \"localPort\": $lp, \"servicePort\": $sp, \"pid\": $pid}" >> "$PIDS_FILE"
done
echo "]}" >> "$PIDS_FILE"

echo "Saved forward mapping & PIDs to $PIDS_FILE"

cat <<'USAGE'

If automatic port-forwards fail or you prefer manual control, run these commands in separate terminals:

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
