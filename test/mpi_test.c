#include <stdio.h>

#include "mpi.h"

#include "gotcha_mpi_io.h"

int main(int argc, char *argv[])
{
  MPI_File fh;
  int buf[1000], rank;

  init_gotcha_mpi_io();

  MPI_Init(0,0);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_File_open(MPI_COMM_WORLD, "test.out", 
                MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

  if (rank == 0)
    MPI_File_write(fh, buf, 1000, MPI_INT, MPI_STATUS_IGNORE);

  MPI_File_close(&fh);
  MPI_Finalize();

  return 0;
}
