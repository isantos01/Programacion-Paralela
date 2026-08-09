# Microservicio Actor Model (Akka) sobre Serverless (AWS Lambda)

Actividad Semana 14 - Modelo de Actores y Arquitecturas Serverless.

Un microservicio construido con el **Modelo de Actores** usando **Akka Typed (Java)**:
un actor **Supervisor** gestiona un grupo de actores **Worker** que procesan tareas
(`sum`, `reverse`). Ante un fallo simulado en un worker, el supervisor lo **reinicia**
automaticamente (estrategia de supervision `restart`). El microservicio se expone como
funcion **serverless** (AWS Lambda + API Gateway) mediante un endpoint HTTP JSON, y se
puede probar tambien en **local** con un servidor HTTP incluido en el JDK.

## Arquitectura

```
Cliente HTTP ──JSON──> API Gateway ──> Lambda (TaskHandler)
                                          │  AskPattern.ask
                                          ▼
                                   SupervisorActor
                              (round-robin + supervision)
                            ┌────────┼────────┐
                            ▼        ▼        ▼
                         worker-1 worker-2 worker-3
```

## Requisitos
- JDK 11+
- Maven 3.8+
- (Despliegue) AWS CLI + AWS SAM CLI, o Serverless Framework

## Compilar
```bash
mvn clean package
# genera target/akka-serverless-actors.jar (fat jar)
```

## Probar en LOCAL (sin nube)
```bash
# arranca el servidor local en http://localhost:8080/task
mvn -q compile exec:java -Dexec.mainClass=com.example.local.LocalServer
```
En otra terminal:
```bash
# Suma
curl -s -X POST localhost:8080/task -H "Content-Type: application/json" \
  -d '{"operation":"sum","numbers":[10,20,12]}'
# -> {"ok":true,"handledByWorker":1,"result":"42"}

# Invertir texto
curl -s -X POST localhost:8080/task -H "Content-Type: application/json" \
  -d '{"operation":"reverse","text":"actores"}'
# -> {"ok":true,"handledByWorker":2,"result":"serotca"}

# Fallo simulado (el worker falla y el supervisor lo reinicia)
curl -s -X POST localhost:8080/task -H "Content-Type: application/json" \
  -d '{"operation":"sum","numbers":[1,2],"forceFail":true}'
# -> {"ok":false,"handledByWorker":3,"error":"Fallo simulado en worker 3"}

# Peticion siguiente: el worker ya reiniciado responde con normalidad
curl -s -X POST localhost:8080/task -H "Content-Type: application/json" \
  -d '{"operation":"sum","numbers":[5,5]}'
# -> {"ok":true,"handledByWorker":1,"result":"10"}
```

## Desplegar en AWS (SAM)
```bash
mvn clean package
sam build
sam deploy --guided        # crea Lambda + API Gateway, imprime la URL
# Probar el endpoint remoto:
curl -s -X POST https://XXXX.execute-api.us-east-1.amazonaws.com/Prod/task \
  -H "Content-Type: application/json" \
  -d '{"operation":"sum","numbers":[10,20,12]}'
```

## Estructura
```
src/main/java/com/example/
  actors/SupervisorActor.java   # supervisor + estrategia de supervision
  actors/WorkerActor.java       # worker + fallo simulado
  lambda/TaskHandler.java       # entrada AWS Lambda (API Gateway proxy)
  local/LocalServer.java        # servidor HTTP local para pruebas
src/main/resources/
  application.conf              # config de Akka
  logback.xml                   # logging
template.yaml                   # AWS SAM
serverless.yml                  # Serverless Framework (alternativa)
```

## Nota de licencias
Se usa **Akka 2.6.x (Apache 2.0)**, libre para uso academico. Para un stack 100%
Apache puede sustituirse por **Apache Pekko** (fork de Akka) cambiando los
identificadores de dependencia y los paquetes `akka.*` por `org.apache.pekko.*`.
