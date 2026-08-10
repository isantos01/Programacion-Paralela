"""AWS Lambda handler que dispara el job de Spark.
En Lambda puro Spark es pesado; la práctica habitual es que la función
serverless LANCE el job en un cluster gestionado (EMR Serverless / Glue) y
devuelva el id/resultado. Aquí se muestra la interfaz HTTP equivalente.
"""
import json
# from spark_job import run_pipelines   # en un runtime con Spark/EMR

def handler(event, context):
    body = json.loads(event.get("body") or "{}")
    array = body.get("array", [])
    if not array:
        return {"statusCode": 422, "body": json.dumps({"ok": False, "error": "array vacio"})}
    # result = run_pipelines(array)      # o disparar EMR Serverless y devolver jobRunId
    result = {"note": "en produccion, dispara EMR Serverless/Glue y retorna jobRunId"}
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, **result})}
