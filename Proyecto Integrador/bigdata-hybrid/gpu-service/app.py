"""Microservicio serverless-style (Flask) de PREPROCESAMIENTO en GPU.
Endpoint:  POST /normalize   body: {"array":[...], "prefer":"auto|gpu|cpu"}
Devuelve el array normalizado (min-max) y el tiempo de cómputo.
Llama a los binarios compilados (CUDA o OpenMP); si no hay GPU, usa CPU.

Variable de entorno SIMULATE_FAILS=N -> las primeras N peticiones responden
500 (para demostrar los reintentos del orquestador de actores).
"""
import json, os, subprocess, tempfile
from flask import Flask, request, jsonify

app = Flask(__name__)
HERE = os.path.dirname(os.path.abspath(__file__))
GPU_BIN = os.path.join(HERE, "normalize")       # CUDA (si existe)
CPU_BIN = os.path.join(HERE, "normalize_cpu")   # OpenMP (siempre)
_fails_left = int(os.environ.get("SIMULATE_FAILS", "0"))

def _run(binary, array):
    """Ejecuta un binario de normalización y devuelve (info, datos)."""
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fi:
        fi.write("\n".join(str(x) for x in array)); inp = fi.name
    outp = inp + ".out"
    proc = subprocess.run([binary, inp, outp], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "fallo del binario")
    info = json.loads(proc.stdout.strip().splitlines()[-1])
    with open(outp) as f:
        data = [float(line) for line in f if line.strip()]
    os.unlink(inp); os.unlink(outp)
    return info, data

@app.get("/health")
def health():
    return jsonify(ok=True, gpu=os.path.exists(GPU_BIN))

@app.post("/normalize")
def normalize():
    global _fails_left
    if _fails_left > 0:                       # fallo simulado para reintentos
        _fails_left -= 1
        return jsonify(ok=False, error="GPU service no disponible (simulado)"), 500

    body = request.get_json(force=True, silent=True) or {}
    array = body.get("array", [])
    prefer = body.get("prefer", "auto")
    if not array:
        return jsonify(ok=False, error="array vacio"), 422

    # Selección de dispositivo con fallback automático a CPU.
    use_gpu = os.path.exists(GPU_BIN) and prefer in ("auto", "gpu")
    try:
        info, data = _run(GPU_BIN if use_gpu else CPU_BIN, array)
    except Exception:
        info, data = _run(CPU_BIN, array)     # fallback

    return jsonify(ok=True, device=info["device"], ms=info["ms"],
                   min=info["min"], max=info["max"], normalized=data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
