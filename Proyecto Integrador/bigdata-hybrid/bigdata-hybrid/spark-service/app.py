"""Microservicio serverless-style (Flask) que LANZA el job de Spark.
Endpoint:  POST /spark   body: {"array":[...]}  (array ya normalizado)
Devuelve resultados y tiempos de los pipelines RDD y DataFrame.
"""
from flask import Flask, request, jsonify
from spark_job import run_pipelines

app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify(ok=True)

@app.post("/spark")
def spark():
    body = request.get_json(force=True, silent=True) or {}
    array = body.get("array", [])
    if not array:
        return jsonify(ok=False, error="array vacio"), 422
    try:
        result = run_pipelines(array)
        return jsonify(ok=True, **result)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
