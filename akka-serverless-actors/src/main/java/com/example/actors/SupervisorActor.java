package com.example.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * SupervisorActor: actor raiz del microservicio.
 *  - Crea un grupo de WorkerActor al arrancar.
 *  - Aplica una ESTRATEGIA DE SUPERVISION: si un worker lanza una
 *    excepcion, Akka lo REINICIA automaticamente (hasta 3 veces/min).
 *  - Reparte las tareas entrantes entre los workers (round-robin).
 */
public class SupervisorActor extends AbstractBehavior<SupervisorActor.Command> {

    /* ============================ PROTOCOLO ============================ */
    public interface Command {}

    /** Peticion de procesamiento que llega desde la capa HTTP/Lambda. */
    public static final class ProcessTask implements Command {
        public final String operation;
        public final List<Integer> numbers;
        public final String text;
        public final boolean forceFail;
        public final ActorRef<Response> replyTo;

        public ProcessTask(String operation, List<Integer> numbers, String text,
                           boolean forceFail, ActorRef<Response> replyTo) {
            this.operation = operation;
            this.numbers = numbers;
            this.text = text;
            this.forceFail = forceFail;
            this.replyTo = replyTo;
        }
    }

    /** Respuesta unificada que devuelve el sistema de actores. */
    public static final class Response {
        public final boolean ok;
        public final String result;
        public final String error;
        public final int handledByWorker;

        private Response(boolean ok, String result, String error, int worker) {
            this.ok = ok; this.result = result; this.error = error; this.handledByWorker = worker;
        }
        public static Response ok(String result, int worker)  { return new Response(true,  result, null,  worker); }
        public static Response error(String error, int worker){ return new Response(false, null,   error, worker); }
    }

    /* Lista de referencias a los workers y puntero round-robin. */
    private final List<ActorRef<WorkerActor.Command>> workers = new ArrayList<>();
    private int next = 0;

    private SupervisorActor(ActorContext<Command> context, int numWorkers) {
        super(context);

        for (int i = 1; i <= numWorkers; i++) {
            /* Envolvemos el comportamiento del worker con una estrategia de
               supervision. onFailure(restart) => ante cualquier excepcion,
               reiniciar el actor conservando su identidad (misma direccion). */
            Behavior<WorkerActor.Command> supervised =
                    Behaviors.supervise(WorkerActor.create(i))
                             .onFailure(SupervisorStrategy.restart()
                                     .withLimit(3, Duration.ofMinutes(1)));

            ActorRef<WorkerActor.Command> worker = context.spawn(supervised, "worker-" + i);
            workers.add(worker);
        }
        context.getLog().info("Supervisor iniciado con {} workers", numWorkers);
    }

    public static Behavior<Command> create(int numWorkers) {
        return Behaviors.setup(ctx -> new SupervisorActor(ctx, numWorkers));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ProcessTask.class, this::onProcessTask)
                .build();
    }

    /* Reparte cada tarea a un worker siguiendo round-robin. */
    private Behavior<Command> onProcessTask(ProcessTask task) {
        int idx = next;                          // worker elegido
        next = (next + 1) % workers.size();      // avanzar el puntero
        ActorRef<WorkerActor.Command> worker = workers.get(idx);

        getContext().getLog().info("Supervisor despacha '{}' -> worker-{}", task.operation, idx + 1);

        /* Reenviamos la tarea; el worker respondera DIRECTAMENTE a replyTo. */
        worker.tell(new WorkerActor.Task(task.operation, task.numbers,
                task.text, task.forceFail, task.replyTo));
        return this;
    }
}
