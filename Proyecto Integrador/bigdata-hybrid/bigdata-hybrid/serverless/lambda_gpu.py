"""AWS Lambda handler que envuelve la normalización.
NOTA: Lambda no tiene GPU; en producción el cómputo GPU iría en un servicio
con GPU (AWS Batch/ECS con instancia g4/g5, o SageMaker). Aquí el handler
llama a la misma lógica y cae a CPU (OpenMP) automáticamente. Deploy con SAM.
"""
import json, subprocess, tempfile, os
CPU_BIN = os.path.join(os.path.dirname(__file__), "normalize_cpu")

def handler(event, context):
    body = json.loads(event.get("body") or "{}")
    array = body.get("array", [])
    if not array:
        return {"statusCode": 422, "body": json.dumps({"ok": False, "error": "array vacio"})}
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fi:
        fi.write("\n".join(str(x) for x in array)); inp = fi.name
    outp = inp + ".out"
    subprocess.run([CPU_BIN, inp, outp], check=True, capture_output=True, text=True)
    data = [float(l) for l in open(outp) if l.strip()]
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "device": "cpu", "normalized": data})}
