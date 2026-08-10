"""Orquestación con el MODELO DE ACTORES (Pykka, equivalente a Akka).
Cada actor gestiona UNA etapa del pipeline y se comunica por mensajes:

  ValidatorActor  -> valida el input
  GpuStageActor   -> llama al servicio GPU (con REINTENTOS)
  SparkStageActor -> llama al servicio Spark (con REINTENTOS)
  AnalyzerActor   -> analiza los resultados y arma la respuesta final

El OrchestratorActor encadena las etapas y maneja los errores. Los reintentos
implementan la tolerancia a fallos típica del modelo de actores.
"""
import time, requests, pykka

GPU_URL   = "http://localhost:5001/normalize"
SPARK_URL = "http://localhost:5002/spark"

def _post_with_retries(url, payload, retries=3, backoff=0.5, log=None):
    """Reintenta una llamada HTTP hasta 'retries' veces con backoff.
    Es el corazón de la tolerancia a fallos por actor."""
    last = None
    for intento in range(1, retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=60)
            if r.status_code == 200:
                if log is not None: log.append(f"    intento {intento}: OK")
                return r.json()
            last = f"HTTP {r.status_code}: {r.json().get('error')}"
        except Exception as e:
            last = str(e)
        if log is not None: log.append(f"    intento {intento} fallo -> {last}; reintentando...")
        time.sleep(backoff * intento)
    raise RuntimeError(f"agotados {retries} reintentos: {last}")

class ValidatorActor(pykka.ThreadingActor):
    def on_receive(self, msg):
        arr = msg.get("array")
        if not isinstance(arr, list) or len(arr) == 0:
            raise ValueError("input invalido: se requiere un array no vacio")
        nums = [float(x) for x in arr]      # lanza si hay no-numéricos
        return {"array": nums, "n": len(nums)}

class GpuStageActor(pykka.ThreadingActor):
    def on_receive(self, msg):
        log = msg["log"]
        log.append("[GPU] emitiendo job de normalizacion...")
        res = _post_with_retries(GPU_URL, {"array": msg["array"]}, log=log)
        log.append(f"[GPU] normalizado en '{res['device']}' ({res['ms']} ms)")
        return res

class SparkStageActor(pykka.ThreadingActor):
    def on_receive(self, msg):
        log = msg["log"]
        log.append("[SPARK] emitiendo job Spark (RDD + DataFrame)...")
        res = _post_with_retries(SPARK_URL, {"array": msg["normalized"]}, log=log)
        log.append(f"[SPARK] RDD={res['rdd']['time_s']}s  DF={res['dataframe']['time_s']}s")
        return res

class AnalyzerActor(pykka.ThreadingActor):
    def on_receive(self, msg):
        spark = msg["spark"]; gpu = msg["gpu"]
        return {
            "n": len(gpu["normalized"]),
            "gpu_device": gpu["device"], "gpu_ms": gpu["ms"],
            "rdd_time_s": spark["rdd"]["time_s"],
            "df_time_s": spark["dataframe"]["time_s"],
            "speedup_df_vs_rdd": spark["speedup_df_vs_rdd"],
            "mean_normalized": spark["dataframe"]["mean"],
            "stddev_normalized": spark["dataframe"]["stddev"],
            "histogram": spark["rdd"]["hist"],
        }

def run_pipeline(array):
    """Ejecuta el pipeline completo a través de los actores y devuelve
    (resultado_final, log_de_ejecucion)."""
    log = []
    validator = ValidatorActor.start()
    gpu = GpuStageActor.start()
    spark = SparkStageActor.start()
    analyzer = AnalyzerActor.start()
    try:
        log.append("[VALID] validando input...")
        v = validator.ask({"array": array})
        log.append(f"[VALID] OK, {v['n']} elementos")

        g = gpu.ask({"array": v["array"], "log": log})
        s = spark.ask({"normalized": g["normalized"], "log": log})
        final = analyzer.ask({"gpu": g, "spark": s})
        log.append("[DONE] pipeline completado")
        return final, log
    finally:
        pykka.ActorRegistry.stop_all()
