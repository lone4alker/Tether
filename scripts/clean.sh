#!/usr/bin/env bash
# scripts/clean.sh - remove Tether workloads and their storage
set -euo pipefail

kubectl delete statefulset tether-broker --ignore-not-found
kubectl delete deployment prometheus grafana --ignore-not-found
kubectl delete service tether-broker tether-broker-headless prometheus grafana --ignore-not-found
kubectl delete configmap prometheus-config grafana-provisioning grafana-dashboards --ignore-not-found
kubectl delete pvc -l app=tether-broker --ignore-not-found

echo "Remaining PVCs (should be empty):"
kubectl get pvc --no-headers 2>/dev/null || echo "  none"