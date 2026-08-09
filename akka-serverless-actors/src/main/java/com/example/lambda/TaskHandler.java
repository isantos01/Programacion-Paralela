package com.example.lambda;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.example.actors.SupervisorActor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * TaskHandler: punto de entrada de AWS Lambda (integracion proxy con API Gateway).
 * Recibe una peticion HTTP con cuerpo JSON, la despacha al sistema de actores
 * y devuelve la respuesta como JSON.
 */
public class TaskHandler
        implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    /* --- CLAVE del matrimonio actores + serverless ---
     * El ActorSystem es un campo STATIC: se crea UNA sola vez por contenedor
     * (cold start) y se REUTILIZA en todas las invocaciones "calientes" (warm).
     * Asi los actores y su estado de supervision sobreviven entre peticiones,
     * pagando el arranque una unica vez. */
    private static final ActorSystem<SupervisorActor.Command> SYSTEM =
            ActorSystem.create(SupervisorActor.create(3), "serverless-actor-system");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request,
                                                      Context context) {
        try {
            /* 1) Parsear el cuerpo JSON de la peticion. */
            String rawBody = (request.getBody() == null) ? "{}" : request.getBody();
            JsonNode body = MAPPER.readTree(rawBody);

            String operation = body.path("operation").asText("sum");
            boolean forceFail = body.path("forceFail").asBoolean(false);
            String text = body.path("text").asText("");
            List<Integer> numbers = new ArrayList<>();
            if (body.has("numbers")) {
                body.get("numbers").forEach(n -> numbers.add(n.asInt()));
            }

            /* 2) Patron ASK: enviar la tarea al supervisor y obtener un Future
                  con la respuesta. Akka crea un actor temporal como replyTo. */
            CompletionStage<SupervisorActor.Response> ask = AskPattern.ask(
                    SYSTEM,
                    replyTo -> new SupervisorActor.ProcessTask(operation, numbers, text, forceFail, replyTo),
                    ASK_TIMEOUT,
                    SYSTEM.scheduler());

            /* 3) Esperar el resultado (ejecucion asincrona resuelta a sincrona
                  para devolver la respuesta HTTP). Con timeout de seguridad. */
            SupervisorActor.Response response =
                    ask.toCompletableFuture().get(6, TimeUnit.SECONDS);

            /* 4) Construir la respuesta JSON. */
            Map<String, Object> out = new HashMap<>();
            out.put("ok", response.ok);
            out.put("handledByWorker", response.handledByWorker);
            if (response.ok) out.put("result", response.result);
            else             out.put("error", response.error);

            return respond(response.ok ? 200 : 422, out);

        } catch (Exception e) {
            /* Tolerancia a fallos: timeout del worker, JSON invalido, etc. */
            context.getLogger().log("ERROR en TaskHandler: " + e.getMessage());
            Map<String, Object> out = new HashMap<>();
            out.put("ok", false);
            out.put("error", "Fallo o timeout procesando la tarea: " + e.getMessage());
            return respond(500, out);
        }
    }

    private APIGatewayProxyResponseEvent respond(int status, Map<String, Object> body) {
        APIGatewayProxyResponseEvent res = new APIGatewayProxyResponseEvent();
        res.setStatusCode(status);
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        res.setHeaders(headers);
        try {
            res.setBody(MAPPER.writeValueAsString(body));
        } catch (Exception e) {
            res.setBody("{\"ok\":false,\"error\":\"serializacion\"}");
        }
        return res;
    }
}
