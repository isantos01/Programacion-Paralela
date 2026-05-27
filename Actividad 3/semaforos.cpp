/*
 * Programa: Sincronización con Secciones Críticas y Semáforos
 * Autor   : Actividad Semana 3 — Programación Concurrente
 * Descripción: Demuestra el acceso concurrente de múltiples hilos
 *              a una variable compartida, usando semáforos POSIX
 *              para garantizar la exclusión mutua.
 */

#include <iostream>
#include <pthread.h>
#include <semaphore.h>
#include <cstdlib>

// ─── Constantes ──────────────────────────────────────────────────────────────
const int NUM_HILOS      = 5;
const int ITERACIONES    = 1000;
const int VALOR_ESPERADO = NUM_HILOS * ITERACIONES;   // 5000

// ─── Recursos compartidos ────────────────────────────────────────────────────
long long contador_compartido = 0;   // Variable bajo protección
sem_t semaforo;                      // Semáforo binario (mutex)

// ─── Función ejecutada por cada hilo ─────────────────────────────────────────
void* incrementar(void* arg) {
    int id = *((int*)arg);
    for (int i = 0; i < ITERACIONES; ++i) {
        // ── Sección de entrada ──
        sem_wait(&semaforo);   // P(s) — adquirir semáforo

        // ── Sección crítica ─────
        ++contador_compartido;

        // ── Sección de salida ───
        sem_post(&semaforo);   // V(s) — liberar semáforo
    }
    std::cout << "[Hilo " << id << "] Completó " << ITERACIONES
              << " incrementos.\n";
    return nullptr;
}

// ─── Demostración SIN sincronización (condición de carrera) ──────────────────
long long contador_sin_sync = 0;

void* incrementar_sin_sync(void* arg) {
    int id = *((int*)arg);
    for (int i = 0; i < ITERACIONES; ++i) {
        ++contador_sin_sync;   // Acceso NO protegido → race condition
    }
    return nullptr;
}

// ─── main ────────────────────────────────────────────────────────────────────
int main() {
    pthread_t hilos[NUM_HILOS];
    int ids[NUM_HILOS];

    std::cout << "========================================\n";
    std::cout << "  Sincronización con Semáforos POSIX    \n";
    std::cout << "========================================\n\n";

    // ── 1. Demo SIN sincronización ──────────────────────────────────────────
    std::cout << "--- PRUEBA SIN sincronización (race condition) ---\n";
    for (int i = 0; i < NUM_HILOS; ++i) {
        ids[i] = i + 1;
        pthread_create(&hilos[i], nullptr, incrementar_sin_sync, &ids[i]);
    }
    for (int i = 0; i < NUM_HILOS; ++i)
        pthread_join(hilos[i], nullptr);

    std::cout << "Valor esperado : " << VALOR_ESPERADO << "\n";
    std::cout << "Valor obtenido : " << contador_sin_sync << "\n";
    std::cout << (contador_sin_sync == VALOR_ESPERADO
                  ? "Resultado: CORRECTO (por suerte)\n"
                  : "Resultado: INCORRECTO — condicion de carrera detectada\n");
    std::cout << "\n";

    // ── 2. Demo CON semáforo ────────────────────────────────────────────────
    std::cout << "--- PRUEBA CON semaforo (sincronizacion correcta) ---\n";

    // Inicializar semáforo binario en 1 (disponible)
    if (sem_init(&semaforo, 0, 1) != 0) {
        std::cerr << "Error al inicializar el semaforo.\n";
        return EXIT_FAILURE;
    }

    for (int i = 0; i < NUM_HILOS; ++i) {
        ids[i] = i + 1;
        pthread_create(&hilos[i], nullptr, incrementar, &ids[i]);
    }
    for (int i = 0; i < NUM_HILOS; ++i)
        pthread_join(hilos[i], nullptr);

    sem_destroy(&semaforo);

    std::cout << "\nValor esperado  : " << VALOR_ESPERADO << "\n";
    std::cout << "Valor obtenido  : " << contador_compartido << "\n";
    std::cout << (contador_compartido == VALOR_ESPERADO
                  ? "Resultado: CORRECTO — sincronizacion exitosa\n"
                  : "Resultado: ERROR inesperado\n");

    std::cout << "\n========================================\n";
    std::cout << "  Fin del programa\n";
    std::cout << "========================================\n";
    return EXIT_SUCCESS;
}
