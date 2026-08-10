"""Punto de entrada HTTP del pipeline (la 'función serverless' orquestadora).
  POST /process       body: {"array":[...]}  -> ejecuta todo y persiste el resultado
  GET  /result/<id>                          -> recupera un resultado persistido
"""
import json, os, uuid
from flask import Flask, request, jsonify
from actors import run_pipeline

app = Flask(__name__)
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

@app.post("/process")
def process():
    body = request.get_json(force=True, silent=True) or {}
    array = body.get("array", [])
    job_id = uuid.uuid4().hex[:8]
    try:
        result, log = run_pipeline(array)
        record = {"job_id": job_id, "status": "ok", "result": result, "log": log}
    except Exception as e:
        record = {"job_id": job_id, "status": "error", "error": str(e)}
    # Persistencia del resultado final.
    with open(os.path.join(RESULTS_DIR, f"{job_id}.json"), "w") as f:
        json.dump(record, f, indent=2)
    return jsonify(record), (200 if record["status"] == "ok" else 500)

@app.get("/result/<job_id>")
def get_result(job_id):
    path = os.path.join(RESULTS_DIR, f"{job_id}.json")
    if not os.path.exists(path):
        return jsonify(error="no encontrado"), 404
    with open(path) as f:
        return jsonify(json.load(f))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
