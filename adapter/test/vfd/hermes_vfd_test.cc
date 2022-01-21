#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <mpi.h>
#include <hdf5.h>
/* HDF5 header for dynamic plugin loading */
#include <H5PLextern.h>

#include "H5FDhermes.h"

void AssertFunc(bool expr, const char *file, int lineno, const char *message) {
  if (!expr) {
    fprintf(stderr, "Assertion failed at %s: line %d: %s\n", file, lineno,
            message);
    exit(-1);
  }
}

#define Assert(expr) AssertFunc((expr), __FILE__, __LINE__, #expr)

int main(int argc, char *argv[]) {
  hid_t file_id;
  hid_t dset_id;
  hid_t dataspace_id;
  hid_t fapl_id;
  hsize_t dims[2];
  const char *file_name = "hermes_test.h5";
  const int kNx = 128;
  const int kNy = 128;
  int data_in[kNx][kNy];  /* data to write */
  int data_out[kNx][kNy];  /* data to read */
  int i, j;

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr,
            "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  const char kDatasetName[] = "IntArray";
  dims[0] = kNx;
  dims[1] = kNy;

  Assert((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) >= 0);
  Assert(H5Pset_fapl_hermes(fapl_id, true, 1024) >= 0);

  Assert((file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT,
                              fapl_id)) >= 0);
  Assert((dataspace_id = H5Screate_simple(2, dims, NULL)) >= 0);
  dset_id = H5Dcreate(file_id, kDatasetName, H5T_NATIVE_INT, dataspace_id,
                      H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  Assert(dset_id >= 0);

  for (j = 0; j < kNx; j++) {
    for (i = 0; i < kNy; i++) {
      data_in[j][i] = i + j;
      data_out[j][i] = 0;
    }
  }

  Assert(H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                  data_in) >= 0);
  Assert(H5Dclose(dset_id) >= 0);
  Assert(H5Fclose(file_id) >= 0);

  hid_t fid;
  Assert((fid = H5Fopen(file_name, H5F_ACC_RDONLY, H5P_DEFAULT)) >= 0);
  Assert((dset_id = H5Dopen(fid, kDatasetName, H5P_DEFAULT)) >= 0);
  Assert(H5Dread(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                 data_out) >= 0);

  for (j = 0; j < kNx; j++) {
    for (i = 0; i < kNy; i++) {
      Assert(data_out[j][i] == data_in[j][i]);
    }
  }

  Assert(H5Dclose(dset_id) >= 0);
  Assert(H5Fclose(fid) >= 0);
  Assert(H5Sclose(dataspace_id) >= 0);
  Assert(H5Pclose(fapl_id) >= 0);
  remove(file_name);

  return 0;
}
