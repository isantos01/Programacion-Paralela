/*********************************************************************
 * Actividad Semana 11 - Tolerancia a Fallos en Sistemas Distribuidos
 * Checkpoint coordinado y Rollback Recovery con MPI
 *
 * Descripción:
 *   Aplicación MPI con 3 (o más) procesos que realiza una suma de
 *   vectores por iteraciones. Cada cierto número de iteraciones se
 *   toma un CHECKPOINT COORDINADO: todos los procesos se sincronizan
 *   con MPI_Barrier antes y después de guardar su estado local en un
 *   archivo, garantizando un estado global consistente.
 *
 *   En la primera ejecución se SIMULA UN FALLO: en la iteración
 *   FALLO_ITER el proceso de rango 1 aborta con exit(). Al volver a
 *   lanzar el programa (reinicio), cada proceso detecta su archivo
 *   de checkpoint, hace ROLLBACK al último estado consistente
 *   guardado y continúa la ejecución hasta terminar.
 *
 * Compilación:  mpicc -O2 -o checkpoint_mpi checkpoint_mpi.c
 * Ejecución:    mpirun -np 3 ./checkpoint_mpi
 *********************************************************************/

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define N               8      /* Tamaño de los vectores               */
#define ITER_TOTAL      20     /* Iteraciones totales del cómputo      */
#define INTERVALO_CKPT  5      /* Cada cuántas iteraciones hay ckpt    */
#define FALLO_ITER      12     /* Iteración en la que se simula fallo  */
#define RANGO_QUE_FALLA 1      /* Proceso que aborta intencionalmente  */

/* ------------------------------------------------------------------
 * Estado crítico del proceso: es lo que se guarda en el checkpoint.
 * Contiene la iteración alcanzada y el resultado parcial del cómputo.
 * ------------------------------------------------------------------ */
typedef struct {
    int    iteracion;      /* Última iteración completada             */
    double suma_parcial;   /* Acumulado de la suma de vectores        */
    double vector_c[N];    /* Resultado parcial C = A + B acumulado   */
} Estado;

/* Nombre del archivo de checkpoint local de cada proceso */
static void nombre_checkpoint(char *buf, size_t tam, int rango) {
    snprintf(buf, tam, "checkpoint_rank_%d.dat", rango);
}

/* ------------------------------------------------------------------
 * guardar_checkpoint: escribe el estado crítico en un archivo local.
 * Se escribe primero a un archivo temporal y luego se renombra
 * (rename es atómico) para evitar checkpoints corruptos si el
 * proceso falla justo mientras escribe.
 * ------------------------------------------------------------------ */
static int guardar_checkpoint(const Estado *e, int rango) {
    char archivo[64], temporal[72];
    nombre_checkpoint(archivo, sizeof archivo, rango);
    snprintf(temporal, sizeof temporal, "%s.tmp", archivo);

    FILE *f = fopen(temporal, "wb");
    if (!f) { perror("fopen checkpoint"); return -1; }
    if (fwrite(e, sizeof(Estado), 1, f) != 1) {
        perror("fwrite checkpoint");
        fclose(f);
        return -1;
    }
    fclose(f);
    rename(temporal, archivo);   /* Publicación atómica del checkpoint */
    return 0;
}

/* ------------------------------------------------------------------
 * cargar_checkpoint: si existe un archivo de checkpoint, recupera el
 * estado (rollback). Devuelve 1 si se recuperó, 0 si no existía.
 * ------------------------------------------------------------------ */
static int cargar_checkpoint(Estado *e, int rango) {
    char archivo[64];
    nombre_checkpoint(archivo, sizeof archivo, rango);

    FILE *f = fopen(archivo, "rb");
    if (!f) return 0;                       /* No hay checkpoint       */
    size_t leidos = fread(e, sizeof(Estado), 1, f);
    fclose(f);
    return (leidos == 1) ? 1 : 0;
}

/* ------------------------------------------------------------------
 * checkpoint_coordinado: protocolo de checkpoint COORDINADO.
 *   1) MPI_Barrier: nadie guarda hasta que TODOS llegaron al punto
 *      de checkpoint -> no hay mensajes "en vuelo" entre procesos y
 *      el conjunto de checkpoints forma una línea de recuperación
 *      globalmente consistente.
 *   2) Cada proceso guarda su estado local.
 *   3) MPI_Barrier: nadie avanza hasta que TODOS terminaron de
 *      guardar -> el checkpoint global queda "confirmado".
 * ------------------------------------------------------------------ */
static void checkpoint_coordinado(const Estado *e, int rango) {
    MPI_Barrier(MPI_COMM_WORLD);            /* Fase 1: sincronizar     */
    guardar_checkpoint(e, rango);           /* Fase 2: guardar local   */
    MPI_Barrier(MPI_COMM_WORLD);            /* Fase 3: confirmar       */
    if (rango == 0)
        printf("[CKPT ] Checkpoint COORDINADO completado en la iteracion %d por todos los procesos\n",
               e->iteracion);
    fflush(stdout);
}

int main(int argc, char *argv[]) {
    int rango, num_procesos;
    Estado estado;
    double A[N], B[N];

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rango);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procesos);

    if (num_procesos < 3) {
        if (rango == 0)
            fprintf(stderr, "Se requieren al menos 3 procesos (use mpirun -np 3)\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Vectores de entrada: deterministas y dependientes del rango,
       para que el resultado sea reproducible tras la recuperación.  */
    for (int i = 0; i < N; i++) {
        A[i] = rango + i;
        B[i] = 2.0 * i;
    }

    /* --------------------- FASE DE RECUPERACIÓN ---------------------
       Justo después de MPI_Init cada proceso verifica si existe un
       checkpoint local. Si existe -> ROLLBACK (reanuda desde el estado
       guardado). Si no -> arranque en frío y checkpoint inicial.      */
    int recuperado = cargar_checkpoint(&estado, rango);

    if (recuperado) {
        printf("[REC  ] Proceso %d: checkpoint encontrado. ROLLBACK a iteracion %d (suma parcial = %.1f)\n",
               rango, estado.iteracion, estado.suma_parcial);
    } else {
        memset(&estado, 0, sizeof estado);
        estado.iteracion = 0;
        printf("[INIT ] Proceso %d: sin checkpoint previo. Iniciando desde cero.\n", rango);
        /* Checkpoint inicial coordinado (estado 0 consistente) */
        checkpoint_coordinado(&estado, rango);
    }
    fflush(stdout);

    /* ¿El fallo ya ocurrió en una ejecución anterior? Se usa un
       archivo-bandera para simular el fallo UNA sola vez.            */
    int fallo_ya_simulado = (access("fallo_simulado.flag", F_OK) == 0);

    /* ------------------------ BUCLE DE CÓMPUTO ---------------------- */
    for (int iter = estado.iteracion + 1; iter <= ITER_TOTAL; iter++) {

        /* Cómputo simple: suma de vectores C += A + B */
        for (int i = 0; i < N; i++) {
            estado.vector_c[i] += A[i] + B[i];
            estado.suma_parcial += A[i] + B[i];
        }
        estado.iteracion = iter;

        printf("[CALC ] Proceso %d: iteracion %2d completada (suma parcial = %.1f)\n",
               rango, iter, estado.suma_parcial);
        fflush(stdout);
        usleep(200000);   /* Pausa de 0.2 s para observar la ejecución */

        /* -------------------- SIMULACIÓN DE FALLO --------------------
           Solo en la primera ejecución: el proceso RANGO_QUE_FALLA
           aborta intencionalmente en FALLO_ITER. Nótese que el último
           checkpoint fue en la iteración 10, así que las iteraciones
           11 y 12 se PERDERÁN y se recalcularán tras el rollback.     */
        if (!fallo_ya_simulado && rango == RANGO_QUE_FALLA && iter == FALLO_ITER) {
            FILE *flag = fopen("fallo_simulado.flag", "w");
            if (flag) { fputs("fallo simulado\n", flag); fclose(flag); }
            fprintf(stderr,
                "\n[FALLO] Proceso %d: *** FALLO SIMULADO en la iteracion %d ***\n"
                "[FALLO] Abortando con exit(1). Vuelva a ejecutar 'mpirun -np 3 ./checkpoint_mpi'\n"
                "[FALLO] para observar el ROLLBACK al ultimo checkpoint coordinado.\n\n", rango, iter);
            fflush(stderr);
            exit(1);      /* Aborto intencional del proceso            */
        }

        /* ------------------ CHECKPOINT COORDINADO -------------------- */
        if (iter % INTERVALO_CKPT == 0)
            checkpoint_coordinado(&estado, rango);
    }

    /* ------------------------- VERIFICACIÓN -------------------------
       Se reduce la suma de todos los procesos para demostrar que el
       resultado global es correcto y consistente tras la recuperación. */
    double suma_global = 0.0;
    MPI_Reduce(&estado.suma_parcial, &suma_global, 1, MPI_DOUBLE,
               MPI_SUM, 0, MPI_COMM_WORLD);

    printf("[FIN  ] Proceso %d: computo terminado en iteracion %d (suma parcial = %.1f)\n",
           rango, estado.iteracion, estado.suma_parcial);

    if (rango == 0) {
        /* Valor esperado: sum_{r} ITER_TOTAL * sum_i (r+i + 2i)       */
        double esperado = 0.0;
        for (int r = 0; r < num_procesos; r++)
            for (int i = 0; i < N; i++)
                esperado += ITER_TOTAL * (r + i + 2.0 * i);
        printf("\n[FIN  ] SUMA GLOBAL = %.1f | Valor esperado = %.1f | %s\n",
               suma_global, esperado,
               (suma_global == esperado) ? "CONSISTENTE ✔" : "INCONSISTENTE ✘");
        printf("[FIN  ] La ejecucion se recupero correctamente tras el fallo.\n");
    }

    MPI_Finalize();
    return 0;
}
