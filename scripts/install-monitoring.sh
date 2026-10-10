#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== 1. Dodawanie repozytorium Helm dla Prometheusa ==="
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

echo "=== 2. Wdrażanie kube-prometheus-stack ==="
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  -f "${ROOT_DIR}/monitoring/helm-values.yaml"

echo "=== 3. Aplikowanie ServiceMonitorów mikroserwisów ==="
kubectl apply -f "${ROOT_DIR}/deploy/k8s/base/servicemonitors.yaml"

echo "=== Monitoring zainstalowany pomyślnie! ==="
echo "Dostęp do Grafany: kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80"
echo "Login: admin, Hasło: admin (lub zdefiniowane w monitoring/helm-values.yaml)"

