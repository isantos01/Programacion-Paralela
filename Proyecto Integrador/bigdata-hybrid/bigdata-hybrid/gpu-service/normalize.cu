// normalize.cu — Normalización min-max de un array en GPU (CUDA) usando
// OpenMP en el host para la reducción min/max. Uso:
//   ./normalize input.txt output.txt
// Imprime una línea JSON con dispositivo, tamaño y tiempo del kernel (ms).
#include <cstdio>
#include <vector>
#include <fstream>
#include <cuda_runtime.h>
#include <omp.h>

// Kernel: cada hilo de la GPU normaliza un elemento del array.
__global__ void normalizeKernel(float* d, int n, float mn, float range) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) d[i] = (d[i] - mn) / range;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "uso: %s input.txt output.txt\n", argv[0]); return 1; }

    std::vector<float> h;
    { std::ifstream f(argv[1]); float v; while (f >> v) h.push_back(v); }
    int n = (int)h.size();
    if (n == 0) { fprintf(stderr, "input vacio\n"); return 1; }

    // min/max en el HOST con OpenMP (CPU paralelo).
    float mn = h[0], mx = h[0];
    #pragma omp parallel for reduction(min:mn) reduction(max:mx)
    for (int i = 0; i < n; i++) { if (h[i] < mn) mn = h[i]; if (h[i] > mx) mx = h[i]; }
    float range = (mx > mn) ? (mx - mn) : 1.0f;

    // Copiar a la GPU y lanzar el kernel de normalización.
    float* d; cudaMalloc(&d, n * sizeof(float));
    cudaMemcpy(d, h.data(), n * sizeof(float), cudaMemcpyHostToDevice);

    cudaEvent_t a, b; cudaEventCreate(&a); cudaEventCreate(&b);
    int threads = 256, blocks = (n + threads - 1) / threads;
    cudaEventRecord(a);
    normalizeKernel<<<blocks, threads>>>(d, n, mn, range);
    cudaEventRecord(b); cudaEventSynchronize(b);
    float ms = 0; cudaEventElapsedTime(&ms, a, b);

    cudaMemcpy(h.data(), d, n * sizeof(float), cudaMemcpyDeviceToHost);
    cudaFree(d);

    { std::ofstream f(argv[2]); for (int i = 0; i < n; i++) f << h[i] << "\n"; }
    printf("{\"device\":\"gpu\",\"n\":%d,\"min\":%.4f,\"max\":%.4f,\"ms\":%.4f}\n", n, mn, mx, ms);
    return 0;
}
