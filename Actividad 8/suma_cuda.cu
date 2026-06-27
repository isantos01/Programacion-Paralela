/* ============================================================
 * suma_cuda.cu
 * Suma de dos vectores grandes usando CUDA (paralelismo en GPU)
 * Actividad Semana 8 - Programacion en GPU con CUDA y OpenMP
 * ============================================================
 *
 * Compilar:
 *   nvcc -O2 suma_cuda.cu -o suma_cuda
 *
 * Ejecutar:
 *   ./suma_cuda
 *
 * Si no tienes GPU local, puedes compilar y correr este archivo
 * gratis en Google Colab (Entorno de ejecucion > Cambiar tipo de
 * entorno de ejecucion > GPU), usando:
 *   !nvcc -O2 suma_cuda.cu -o suma_cuda
 *   !./suma_cuda
 */

#include <stdio.h>
#include <stdlib.h>
#include <cuda_runtime.h>

#define N 1048576          /* 1,048,576 = 1M elementos */
#define HILOS_POR_BLOQUE 256

/* ----------------------------------------------------------
 * Macro para chequear errores de CUDA despues de cada llamada.
 * Es buena practica en CUDA: las llamadas no siempre fallan de
 * forma ruidosa, asi que conviene verificar el codigo de error
 * que devuelven cudaMalloc, cudaMemcpy, etc.
 * ---------------------------------------------------------- */
#define CUDA_CHECK(llamada)                                          \
    do {                                                             \
        cudaError_t err = (llamada);                                 \
        if (err != cudaSuccess) {                                    \
            fprintf(stderr, "Error CUDA en %s:%d -> %s\n",            \
                    __FILE__, __LINE__, cudaGetErrorString(err));     \
            exit(EXIT_FAILURE);                                       \
        }                                                             \
    } while (0)

/* ----------------------------------------------------------
 * Kernel: cada hilo de la GPU calcula UNA sola posicion i del
 * vector resultado. El indice global "i" se obtiene combinando
 * el indice de bloque, el indice de hilo dentro del bloque y el
 * tamano de bloque. Esto es el patron clasico "grid-stride
 * free" de un solo elemento por hilo.
 * ---------------------------------------------------------- */
__global__ void add_vectors(const float *A, const float *B, float *C, int n) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;

    /* Como n puede no ser multiplo exacto de hilos_por_bloque,
     * siempre se debe verificar que i no se salga del arreglo. */
    if (i < n) {
        C[i] = A[i] + B[i];
    }
}

int main(void) {
    size_t bytes = N * sizeof(float);

    /* ----------------------------------------------------------
     * 1. Reservar y llenar los vectores en memoria del HOST (CPU)
     * ---------------------------------------------------------- */
    float *h_A = (float *) malloc(bytes);
    float *h_B = (float *) malloc(bytes);
    float *h_C = (float *) malloc(bytes);

    for (int i = 0; i < N; i++) {
        h_A[i] = (float) i;
        h_B[i] = (float) (N - i);
    }

    /* ----------------------------------------------------------
     * 2. Eventos CUDA para medir tiempo con precision en GPU.
     *    Se usan cudaEvent en vez de CPU timers porque el GPU
     *    trabaja de forma asincrona; los eventos se insertan en
     *    el stream y miden el tiempo real transcurrido en GPU.
     * ---------------------------------------------------------- */
    cudaEvent_t inicio_total, fin_total;
    CUDA_CHECK(cudaEventCreate(&inicio_total));
    CUDA_CHECK(cudaEventCreate(&fin_total));

    CUDA_CHECK(cudaEventRecord(inicio_total));

    /* ----------------------------------------------------------
     * 3. Reservar memoria en el DEVICE (GPU)
     * ---------------------------------------------------------- */
    float *d_A, *d_B, *d_C;
    CUDA_CHECK(cudaMalloc((void **) &d_A, bytes));
    CUDA_CHECK(cudaMalloc((void **) &d_B, bytes));
    CUDA_CHECK(cudaMalloc((void **) &d_C, bytes));

    /* ----------------------------------------------------------
     * 4. Copiar datos del HOST al DEVICE
     *    Esta transferencia por el bus PCIe es uno de los
     *    cuellos de botella tipicos en GPU computing.
     * ---------------------------------------------------------- */
    CUDA_CHECK(cudaMemcpy(d_A, h_A, bytes, cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_B, h_B, bytes, cudaMemcpyHostToDevice));

    /* ----------------------------------------------------------
     * 5. Configurar y lanzar el kernel
     *    - hilos_por_bloque = 256 (multiplo de 32, tamano de warp)
     *    - bloques = techo(N / hilos_por_bloque) para cubrir
     *      todos los elementos aunque N no sea multiplo exacto.
     * ---------------------------------------------------------- */
    int hilos_por_bloque = HILOS_POR_BLOQUE;
    int bloques = (N + hilos_por_bloque - 1) / hilos_por_bloque;

    add_vectors<<<bloques, hilos_por_bloque>>>(d_A, d_B, d_C, N);
    CUDA_CHECK(cudaGetLastError());      /* error de lanzamiento */
    CUDA_CHECK(cudaDeviceSynchronize()); /* esperar a que termine */

    /* ----------------------------------------------------------
     * 6. Copiar el resultado del DEVICE al HOST
     * ---------------------------------------------------------- */
    CUDA_CHECK(cudaMemcpy(h_C, d_C, bytes, cudaMemcpyDeviceToHost));

    CUDA_CHECK(cudaEventRecord(fin_total));
    CUDA_CHECK(cudaEventSynchronize(fin_total));

    float tiempo_gpu_ms = 0.0f;
    CUDA_CHECK(cudaEventElapsedTime(&tiempo_gpu_ms, inicio_total, fin_total));

    /* ----------------------------------------------------------
     * 7. Verificacion de resultados en el HOST
     * ---------------------------------------------------------- */
    int correcto = 1;
    for (int i = 0; i < N; i++) {
        if (h_C[i] != h_A[i] + h_B[i]) {
            correcto = 0;
            printf("ERROR en posicion %d: C[%d] = %f, esperado %f\n",
                   i, i, h_C[i], h_A[i] + h_B[i]);
            break;
        }
    }

    /* ----------------------------------------------------------
     * 8. Reporte de resultados
     * ---------------------------------------------------------- */
    printf("================================================\n");
    printf(" Suma de vectores - Version CUDA (GPU)\n");
    printf("================================================\n");
    printf("Tamano del vector (N):           %d\n", N);
    printf("Hilos por bloque:                %d\n", hilos_por_bloque);
    printf("Numero de bloques:               %d\n", bloques);
    printf("Tiempo total GPU (con transf.):  %.6f ms\n", tiempo_gpu_ms);
    printf("Verificacion de resultados:      %s\n",
           correcto ? "CORRECTA" : "FALLIDA");
    printf("Ejemplo: C[0]=%.1f  C[N/2]=%.1f  C[N-1]=%.1f\n",
           h_C[0], h_C[N/2], h_C[N-1]);
    printf("================================================\n");

    /* ----------------------------------------------------------
     * 9. Liberar memoria (device y host) y destruir eventos
     * ---------------------------------------------------------- */
    CUDA_CHECK(cudaFree(d_A));
    CUDA_CHECK(cudaFree(d_B));
    CUDA_CHECK(cudaFree(d_C));
    CUDA_CHECK(cudaEventDestroy(inicio_total));
    CUDA_CHECK(cudaEventDestroy(fin_total));

    free(h_A);
    free(h_B);
    free(h_C);

    return 0;
}
