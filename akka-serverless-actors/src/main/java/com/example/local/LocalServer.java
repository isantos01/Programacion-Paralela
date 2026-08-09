package com.example.local;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import com.example.actors.SupervisorActor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * LocalServer: servidor HTTP LOCAL para probar el microservicio sin desplegar
 * en la nube. Usa el HttpServer incluido en el JDK (sin dependencias extra) y
 * reutiliza EXACTAMENTE el mismo sistema de actores que la version Lambda.
 *
 * Arranque:  mvn -q compile exec:java -Dexec.mainClass=com.example.local.LocalServer
 *   (o ejecutar la clase con el classpath del proyecto)
 * Endpoint:  POST http://localhost:8080/task
 */
public class LocalServer {

    private static final ActorSystem<SupervisorActor.Command> SYSTEM =
            ActorSystem.create(SupervisorActor.create(3), "local-actor-system");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        server.createContext("/task", exchange -> {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                write(exchange, 405, "{\"error\":\"usar POST\"}");
                return;
            }
            try (InputStream is = exchange.getRequestBody()) {
                String rawBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                JsonNode body = MAPPER.readTree(rawBody.isEmpty() ? "{}" : rawBody);

                String operation = body.path("operation").asText("sum");
                boolean forceFail = body.path("forceFail").asBoolean(false);
                String text = body.path("text").asText("");
                List<Integer> numbers = new ArrayList<>();
                if (body.has("numbers")) body.get("numbers").forEach(n -> numbers.add(n.asInt()));

                CompletionStage<SupervisorActor.Response> ask = AskPattern.ask(
                        SYSTEM,
                        replyTo -> new SupervisorActor.ProcessTask(operation, numbers, text, forceFail, replyTo),
                        ASK_TIMEOUT,
                        SYSTEM.scheduler());

                SupervisorActor.Response r = ask.toCompletableFuture().get(6, TimeUnit.SECONDS);

                Map<String, Object> out = new HashMap<>();
                out.put("ok", r.ok);
                out.put("handledByWorker", r.handledByWorker);
                if (r.ok) out.put("result", r.result); else out.put("error", r.error);
                write(exchange, r.ok ? 200 : 422, MAPPER.writeValueAsString(out));

            } catch (Exception e) {
                write(exchange, 500, "{\"ok\":false,\"error\":\"" + e.getMessage() + "\"}");
            }
        });

        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("Servidor local escuchando en http://localhost:8080/task");
    }

    private static void write(com.sun.net.httpserver.HttpExchange ex, int status, String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }
}
