package com.example.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.PreRestart;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.List;

/**
 * WorkerActor: actor "trabajador". Recibe una tarea, la procesa y responde.
 * Es un actor AISLADO: su unica forma de comunicarse es por mensajes
 * inmutables; no comparte memoria con otros actores (no hay locks).
 */
public class WorkerActor extends AbstractBehavior<WorkerActor.Command> {

    /* ============================ PROTOCOLO ============================ *
     * El protocolo del actor es el conjunto de mensajes que acepta.
     * Marcamos la interfaz sellada logicamente con un unico tipo: Task.  */
    public interface Command {}

    /** Mensaje de tarea. Es inmutable (todos los campos final). */
    public static final class Task implements Command {
        public final String operation;                        // "sum" | "reverse"
        public final List<Integer> numbers;                   // datos para "sum"
        public final String text;                             // datos para "reverse"
        public final boolean forceFail;                       // dispara fallo simulado
        public final ActorRef<SupervisorActor.Response> replyTo; // a quien responder

        public Task(String operation, List<Integer> numbers, String text,
                    boolean forceFail, ActorRef<SupervisorActor.Response> replyTo) {
            this.operation = operation;
            this.numbers = numbers;
            this.text = text;
            this.forceFail = forceFail;
            this.replyTo = replyTo;
        }
    }

    private final int workerId;   // identificador del worker (1..N)

    /* El constructor es privado: los actores se crean con create() +
       Behaviors.setup, nunca con "new" directo desde fuera. */
    private WorkerActor(ActorContext<Command> context, int workerId) {
        super(context);
        this.workerId = workerId;
        context.getLog().info("Worker {} iniciado", workerId);
    }

    /** Factory: devuelve el Behavior inicial del actor. */
    public static Behavior<Command> create(int workerId) {
        return Behaviors.setup(ctx -> new WorkerActor(ctx, workerId));
    }

    /* Define que mensajes/senales atiende el actor. */
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Task.class, this::onTask)         // mensaje de negocio
                .onSignal(PreRestart.class, s -> onPreRestart()) // senal del ciclo de vida
                .onSignal(PostStop.class, s -> onPostStop())
                .build();
    }

    /* ------------------------ LOGICA DE NEGOCIO ------------------------ */
    private Behavior<Command> onTask(Task task) {
        getContext().getLog().info("Worker {} recibe operacion '{}'", workerId, task.operation);

        /* --- FALLO SIMULADO ---
         * Si la peticion pide forzar un error: primero respondemos al
         * cliente con un mensaje de error (para que no quede esperando) y
         * luego lanzamos una excepcion. La excepcion es capturada por la
         * estrategia de supervision del padre, que REINICIA este worker. */
        if (task.forceFail) {
            task.replyTo.tell(SupervisorActor.Response.error(
                    "Fallo simulado en worker " + workerId, workerId));
            getContext().getLog().error("Worker {} lanza excepcion (fallo intencional)", workerId);
            throw new RuntimeException("Fallo intencional en worker " + workerId);
        }

        /* --- Procesamiento normal --- */
        SupervisorActor.Response response;
        switch (task.operation) {
            case "sum":
                int total = task.numbers.stream().mapToInt(Integer::intValue).sum();
                response = SupervisorActor.Response.ok(String.valueOf(total), workerId);
                break;
            case "reverse":
                String invertido = new StringBuilder(task.text).reverse().toString();
                response = SupervisorActor.Response.ok(invertido, workerId);
                break;
            default:
                response = SupervisorActor.Response.error(
                        "Operacion no soportada: " + task.operation, workerId);
        }

        task.replyTo.tell(response);   // enviamos la respuesta al solicitante
        return this;                   // el actor mantiene el mismo comportamiento
    }

    /* Senal que Akka envia JUSTO ANTES de reiniciar el actor tras un fallo. */
    private Behavior<Command> onPreRestart() {
        getContext().getLog().warn("Worker {} PRE-RESTART: el supervisor lo esta reiniciando", workerId);
        return this;
    }

    private Behavior<Command> onPostStop() {
        getContext().getLog().info("Worker {} detenido (PostStop)", workerId);
        return this;
    }
}
