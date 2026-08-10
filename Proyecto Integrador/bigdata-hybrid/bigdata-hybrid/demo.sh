#!/usr/bin/env bash
# Envía peticiones de demostración al pipeline completo.
set -e
ARR=$(python3 -c "import json,random;print(json.dumps([random.randint(0,10000) for _ in range(5000)]))")
echo "=== POST /process (pipeline completo) ==="
curl -s -X POST localhost:5000/process -H "Content-Type: application/json" \
  -d "{\"array\": $ARR}" | python3 -m json.tool
