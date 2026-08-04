#!/bin/bash
set -euo pipefail

CLUSTER=kafka-lab
TAG="v$(date +%s)"
IMAGE="kafkaproj:$TAG"


echo "=== Build $IMAGE ==="
docker build -t "$IMAGE" .

echo "=== Caricamento nei nodi kind ==="
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "=== Aggiornamento dei workload ==="
# Un tag nuovo a ogni build: niente ambiguita' su quale immagine sta girando.
kubectl set image statefulset/kafka kafka="$IMAGE"
kubectl set image deployment/publisher publisher="$IMAGE"
kubectl set image deployment/consumer  consumer="$IMAGE"

kubectl rollout status statefulset/kafka   --timeout=180s
kubectl rollout status deployment/publisher --timeout=120s
kubectl rollout status deployment/consumer  --timeout=120s

echo
echo "=== Immagini attive ==="
kubectl get pods -o custom-columns=\
'NOME:.metadata.name,IMMAGINE:.spec.containers[0].image,STATO:.status.phase,RESTART:.status.containerStatuses[0].restartCount'

echo
echo "Controlla che il consumer committi davvero:"
echo "  kubectl logs -l app=consumer --tail=20 | grep Commit"