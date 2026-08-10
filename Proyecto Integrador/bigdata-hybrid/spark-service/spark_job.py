"""Job de Spark con DOS pipelines (RDD y DataFrame) sobre el dataset
preprocesado. Calcula estadísticos y mide el tiempo de cada uno para
comparar rendimiento y speedup.  Uso directo:  python spark_job.py datos.txt
"""
import sys, time
from pyspark.sql import SparkSession, functions as F

_spark = None
def get_spark():
    """Crea (una sola vez) y reutiliza la SparkSession — evita el arranque
    repetido en cada petición (patrón 'warm' de serverless)."""
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder.master("local[*]")
                  .appName("bigdata-hybrid")
                  .config("spark.ui.enabled", "false").getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark

def run_pipelines(numbers):
    """Ejecuta el pipeline RDD y el DataFrame sobre 'numbers' (ya normalizados).
    Devuelve resultados y tiempos de ambos, más el speedup."""
    spark = get_spark(); sc = spark.sparkContext

    # -------- Pipeline 1: RDD --------
    t0 = time.time()
    rdd = sc.parallelize(numbers, 4)
    count = rdd.count()
    total = rdd.sum()
    mean_rdd = total / count
    # histograma simple en 5 buckets [0,0.2,...,1.0]
    hist_rdd = (rdd.map(lambda x: (min(int(x * 5), 4), 1))
                   .reduceByKey(lambda a, b: a + b).collectAsMap())
    t_rdd = time.time() - t0

    # -------- Pipeline 2: DataFrame --------
    t0 = time.time()
    df = spark.createDataFrame([(float(x),) for x in numbers], ["val"])
    agg = df.agg(F.count("val").alias("c"), F.sum("val").alias("s"),
                 F.mean("val").alias("m"), F.stddev("val").alias("sd")).collect()[0]
    mean_df = agg["m"]
    t_df = time.time() - t0

    return {
        "rdd":  {"count": count, "sum": round(total, 4),
                 "mean": round(mean_rdd, 6), "hist": {str(k): v for k, v in sorted(hist_rdd.items())},
                 "time_s": round(t_rdd, 4)},
        "dataframe": {"count": agg["c"], "sum": round(agg["s"], 4),
                      "mean": round(mean_df, 6), "stddev": round(agg["sd"] or 0.0, 6),
                      "time_s": round(t_df, 4)},
        "speedup_df_vs_rdd": round(t_rdd / t_df, 3) if t_df > 0 else None,
    }

if __name__ == "__main__":
    nums = [float(l) for l in open(sys.argv[1]) if l.strip()]
    import json; print(json.dumps(run_pipelines(nums), indent=2))
