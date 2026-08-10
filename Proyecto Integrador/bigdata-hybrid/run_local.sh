#!/usr/bin/env bash
# Levanta los 3 microservicios en local. Ctrl+C para detener todos.
# Opcional: SIMULATE_FAILS=2 ./run_local.sh  (para demostrar reintentos)
set -e
cd "$(dirname "$0")"
bash gpu-service/build.sh
echo "Arrancando GPU (5001), Spark (5002) y Orchestrator (5000)..."
SIMULATE_FAILS=${SIMULATE_FAILS:-0} python3 gpu-service/app.py & P1=$!
( cd spark-service && python3 app.py ) & P2=$!
( cd orchestrator && python3 app.py ) & P3=$!
trap "kill $P1 $P2 $P3 2>/dev/null" EXIT
echo "Listo. Spark tarda ~10-20s en calentar. Prueba con ./demo.sh"
wait
