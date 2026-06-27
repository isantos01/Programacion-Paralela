/* ============================================================
 * suma_omp.c
 * Suma de dos vectores grandes usando OpenMP (paralelismo en CPU)
 * Actividad Semana 8 - Programacion en GPU con CUDA y OpenMP
 * ============================================================
 *
 * Compilar:
 *   gcc -fopenmp -O2 suma_omp.c -o suma_omp
 *
 * Ejecutar (puedes variar el numero de hilos):
 *   OMP_NUM_THREADS=4 ./suma_omp
 */

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

#define N 1048576  /* 1,048,576 = 1M elementos */

int main(int argc, char *argv[]) {
    /* ----------------------------------------------------------
     * 1. Reservar memoria para los tres vectores en el HEAP.
     *    Con N = 1M floats, cada arreglo pesa ~4 MB, por lo que
     *    no caben comodamente en la pila (stack), de ahi el malloc.
     * ---------------------------------------------------------- */
    float *A = (float *) malloc(N * sizeof(float));
    float *B = (float *) malloc(N * sizeof(float));
    float *C = (float *) malloc(N * sizeof(float));

    if (A == NULL || B == NULL || C == NULL) {
        fprintf(stderr, "Error: no se pudo reservar memoria.\n");
        return 1;
    }

    /* ----------------------------------------------------------
     * 2. Inicializar los vectores A y B con valores de ejemplo.
     *    Esta inicializacion tambien se puede paralelizar, pero
     *    aqui la dejamos secuencial para que el tiempo medido
     *    corresponda unicamente a la suma.
     * ---------------------------------------------------------- */
    for (int i = 0; i < N; i++) {
        A[i] = (float) i;
        B[i] = (float) (N - i);
    }

    /* ----------------------------------------------------------
     * 3. Suma paralela: C[i] = A[i] + B[i]
     *    #pragma omp parallel for reparte las N iteraciones del
     *    bucle entre los hilos disponibles. Como cada iteracion
     *    es independiente (no hay dependencias entre i), esto es
     *    "embarrassingly parallel": ideal para OpenMP.
     * ---------------------------------------------------------- */
    double t_inicio = omp_get_wtime();

    #pragma omp parallel for schedule(static)
    for (int i = 0; i < N; i++) {
        C[i] = A[i] + B[i];
    }

    double t_fin = omp_get_wtime();
    double tiempo_omp = t_fin - t_inicio;

    /* ----------------------------------------------------------
     * 4. Verificacion de resultados.
     *    Recorremos el vector y confirmamos que cada posicion
     *    cumple C[i] == A[i] + B[i]. Si alguna falla, marcamos
     *    error. Esto es la prueba de correctitud pedida.
     * ---------------------------------------------------------- */
    int correcto = 1;
    for (int i = 0; i < N; i++) {
        if (C[i] != A[i] + B[i]) {
            correcto = 0;
            printf("ERROR en posicion %d: C[%d] = %f, esperado %f\n",
                   i, i, C[i], A[i] + B[i]);
            break;
        }
    }

    /* ----------------------------------------------------------
     * 5. Reporte de resultados
     * ---------------------------------------------------------- */
    int num_hilos = omp_get_max_threads();

    printf("================================================\n");
    printf(" Suma de vectores - Version OpenMP (CPU)\n");
    printf("================================================\n");
    printf("Tamano del vector (N):       %d\n", N);
    printf("Hilos OpenMP utilizados:     %d\n", num_hilos);
    printf("Tiempo de ejecucion (suma):  %.6f segundos\n", tiempo_omp);
    printf("Verificacion de resultados:  %s\n",
           correcto ? "CORRECTA" : "FALLIDA");
    printf("Ejemplo: C[0]=%.1f  C[N/2]=%.1f  C[N-1]=%.1f\n",
           C[0], C[N/2], C[N-1]);
    printf("================================================\n");

    /* ----------------------------------------------------------
     * 6. Liberar memoria
     * ---------------------------------------------------------- */
    free(A);
    free(B);
    free(C);

    return 0;
}
