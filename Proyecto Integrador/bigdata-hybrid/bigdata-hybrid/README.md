# Aplicación Híbrida de Procesamiento Big-Data en Entorno Serverless

Proyecto integrador que combina **GPU (CUDA/OpenMP)**, **Spark (RDD y DataFrame)**,
**Modelo de Actores** (Pykka, equivalente a Akka) y **arquitectura serverless**.

## Arquitectura

```
Cliente HTTP ─JSON─> Orchestrator (:5000, actores Pykka)
                         │  ValidatorActor -> GpuStageActor -> SparkStageActor -> AnalyzerActor
                         │        (reintentos + tolerancia a fallos)
             ┌───────────┴───────────┐
             ▼                       ▼
     GPU service (:5001)      Spark service (:5002)
   CUDA + OpenMP (normaliza)  RDD + DataFrame (analiza)
             │
        resultados persistidos en results/<job_id>.json
```

## Componentes
- `gpu-service/` — normalización min-max: `normalize.cu` (CUDA+OpenMP) y `normalize_cpu.c` (OpenMP). `app.py` expone `POST /normalize` con **fallback automático a CPU** si no hay GPU.
- `spark-service/` — `spark_job.py` con pipelines **RDD** y **DataFrame** y speedup; `app.py` expone `POST /spark`.
- `orchestrator/` — actores Pykka (`actors.py`) que coordinan las etapas con **reintentos**; `app.py` expone `POST /process` y `GET /result/<id>` con **persistencia**.
- `serverless/` — handlers AWS Lambda + `template.yaml` (SAM).
- `colab/RunOnColab.ipynb` — corre la parte **GPU gratis en Google Colab** (T4).
- `data/generate_dataset.py` — genera datasets de prueba.

## Requisitos
- Python 3.10+ y Java 8/11/17 (para PySpark).
- `pip install -r requirements.txt`
- GPU/CUDA **opcional** (sin ella, todo corre en CPU vía OpenMP).

## Ejecución local (3 microservicios)
```bash
pip install -r requirements.txt
bash gpu-service/build.sh            # compila kernels (CPU siempre; GPU si hay nvcc)
python data/generate_dataset.py 100000 data/input.txt

# Terminal 1: levantar los 3 servicios (para demo de reintentos: SIMULATE_FAILS=2 ./run_local.sh)
./run_local.sh

# Terminal 2: disparar el pipeline completo
./demo.sh
# o manual:
curl -s -X POST localhost:5000/process -H "Content-Type: application/json" \
     -d '{"array":[10,500,999,3,750]}' | python3 -m json.tool
```

## Probar servicios por separado
```bash
curl -s -X POST localhost:5001/normalize -H "Content-Type: application/json" -d '{"array":[0,5,10]}'
curl -s -X POST localhost:5002/spark     -H "Content-Type: application/json" -d '{"array":[0,0.5,1]}'
```

## GPU en la nube (gratis)
Sube `colab/RunOnColab.ipynb` a Google Colab, activa GPU (T4) y ejecuta: compila el
kernel CUDA, genera 5M de números y muestra el **speedup GPU vs CPU**.

## Despliegue serverless (AWS)
```bash
cd serverless && sam build && sam deploy --guided
```

## Nota
"Akka o equivalente": se usa **Pykka** (mismo modelo de actores: buzón, un mensaje
a la vez, aislamiento) para mantener todo el stack en Python y facilitar la demo.
