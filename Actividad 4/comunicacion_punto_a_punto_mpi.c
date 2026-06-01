#include <stdio.h>
#include <mpi.h>

int main(int argc, char *argv[]) {
    int rank, size;
    int valor = 100;
    int emisor = 0;
    int receptor = 1;
    int etiqueta = 0;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 2) {
        if (rank == 0) {
            printf("Este programa necesita al menos 2 procesos para ejecutarse.\n");
            printf("Ejemplo: mpirun -np 2 ./comunicacion_mpi\n");
        }
        MPI_Finalize();
        return 0;
    }

    if (rank == emisor) {
        MPI_Send(&valor, 1, MPI_INT, receptor, etiqueta, MPI_COMM_WORLD);
        printf("Proceso %d envio el valor %d al proceso %d.\n", rank, valor, receptor);
    } else if (rank == receptor) {
        MPI_Recv(&valor, 1, MPI_INT, emisor, etiqueta, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Proceso %d recibio el valor %d desde el proceso %d.\n", rank, valor, emisor);
    }

    MPI_Finalize();
    return 0;
}
