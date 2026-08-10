// normalize_cpu.c — Normalización min-max SOLO en CPU con OpenMP.
// Sirve para comparar GPU vs CPU (speedup). Uso: ./normalize_cpu in.txt out.txt
#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "uso: %s input.txt output.txt\n", argv[0]); return 1; }
    FILE* fi = fopen(argv[1], "r");
    if (!fi) { fprintf(stderr, "no abre input\n"); return 1; }

    int cap = 1024, n = 0; float* a = malloc(cap * sizeof(float)), v;
    while (fscanf(fi, "%f", &v) == 1) {
        if (n == cap) { cap *= 2; a = realloc(a, cap * sizeof(float)); }
        a[n++] = v;
    }
    fclose(fi);
    if (n == 0) { fprintf(stderr, "input vacio\n"); return 1; }

    float mn = a[0], mx = a[0];
    #pragma omp parallel for reduction(min:mn) reduction(max:mx)
    for (int i = 0; i < n; i++) { if (a[i] < mn) mn = a[i]; if (a[i] > mx) mx = a[i]; }
    float range = (mx > mn) ? (mx - mn) : 1.0f;

    double t0 = omp_get_wtime();
    #pragma omp parallel for
    for (int i = 0; i < n; i++) a[i] = (a[i] - mn) / range;
    double t1 = omp_get_wtime();

    FILE* fo = fopen(argv[2], "w");
    for (int i = 0; i < n; i++) fprintf(fo, "%f\n", a[i]);
    fclose(fo);

    printf("{\"device\":\"cpu\",\"n\":%d,\"min\":%.4f,\"max\":%.4f,\"ms\":%.4f,\"threads\":%d}\n",
           n, mn, mx, (t1 - t0) * 1000.0, omp_get_max_threads());
    free(a); return 0;
}
