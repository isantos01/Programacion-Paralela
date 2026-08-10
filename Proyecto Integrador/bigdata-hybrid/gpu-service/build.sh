#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
gcc -O2 -fopenmp normalize_cpu.c -o normalize_cpu
echo "[build] normalize_cpu (OpenMP) OK"
if command -v nvcc >/dev/null 2>&1; then
  nvcc -O2 -Xcompiler -fopenmp normalize.cu -o normalize
  echo "[build] normalize (CUDA) OK"
else
  echo "[build] nvcc no encontrado: se usara solo CPU (fallback automatico)"
fi
