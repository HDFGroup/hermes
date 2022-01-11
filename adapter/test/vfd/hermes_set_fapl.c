#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <mpi.h>
#include <assert.h>

#include "hdf5.h"
#include "H5FDhermes.h"

#define DATASETNAME "IntArray"
#define NX     128                      /* dataset dimensions */
#define NY     128

int main(int argc, char *argv[]) {
    hid_t       file_id, fapl_id;
    hid_t       dset_id, dcpl_id, dataspace_id;
    hid_t       dapl;
    hsize_t     dims[2];
    char       *file_name = "hermes_test.h5";
    int         data_in[NX][NY];          /* data to write */
    int         data_out[NX][NY];         /* data to read */
    int         i, j, k;

    int mpi_threads_provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
    if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr,
                "Didn't receive appropriate MPI threading specification\n");
        return 1;
    }

    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0)
        printf("H5Pcreate() error\n");
    else
        printf("H5Pcreate() succeeded\n");

    if (H5Pset_fapl_hermes(fapl_id, true, 1024) < 0)
        printf("H5Pset_fapl_hermes() error\n");
    else
        printf("H5Pset_fapl_hermes() succeeded\n");

    if ((file_id = H5Fcreate(file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id))
        < 0)
        printf("H5Fcreate() error\n");
    else
        printf("H5Fcreate() succeeded\n");

    dims[0] = NX;
    dims[1] = NY;
    dataspace_id = H5Screate_simple(2, dims, NULL);

    dset_id = H5Dcreate(file_id, DATASETNAME, H5T_NATIVE_INT, dataspace_id,
                        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    for (j = 0; j < NX; j++) {
        for (i = 0; i < NY; i++) {
            data_in[j][i] = i + j;
            data_out[j][i] = 0;
        }
    }

    if (H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                 data_in) < 0)
        printf("H5Dwrite() error\n");
    else
        printf("H5Dwrite() succeeded\n");

    if (H5Dclose(dset_id) < 0)
        printf("H5Dclose() error\n");
    else
        printf("H5Dclose() succeeded\n");

    if (H5Fclose(file_id) < 0)
        printf("H5Fclose() error\n");
    else
        printf("H5Fclose() succeeded\n");

    hid_t fid;
    if ((fid = H5Fopen(file_name, H5F_ACC_RDONLY, fapl_id)) < 0)
        printf("H5Fopen() error\n");
    else
        printf("H5Fopen() succeeded\n");

    if ((dset_id = H5Dopen(fid, DATASETNAME, H5P_DEFAULT)) < 0)
        printf("H5Dopen() error\n");
    else
        printf("H5Dopen() succeeded\n");

    if (H5Dread(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                data_out) < 0)
        printf("H5Dread() error\n");
    else
        printf("H5Dread() succeeded\n");

    for (j = 0; j < NX; j++) {
        for (i = 0; i < NY; i++) {
            assert(data_out[j][i] == data_in[j][i]);
        }
    }

    if (H5Dclose(dset_id) < 0)
        printf("H5Dclose() error\n");
    else
        printf("H5Dclose() succeeded\n");

    if (H5Fclose(fid) < 0)
        printf("H5Fclose() error\n");
    else
        printf("H5Fclose() succeeded\n");

    if (H5Sclose(dataspace_id) < 0)
        printf("H5Sclose() error\n");
    else
        printf("H5Sclose() succeeded\n");

    if (H5Pclose(fapl_id) < 0)
        printf("H5Pclose() error\n");
    else
        printf("H5Pclose() succeeded\n");

    printf("Done with Hermes set driver test!\n");

    return 0;
}
