#include <assert.h>

#include <mpi.h>

#include "common.h"
#include "hermes.h"
#include "bucket.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"

namespace hapi = hermes::api;

int main(int argc, char **argv) {

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  hermes::SharedMemoryContext context = {};

  char fname_template[] = "hermes-buffering-loop-test-XXXXXX";
  FILE *file_handle = 0;

  if (world_rank == 0) {
    hermes::Config config = {};
    InitTestConfig(&config);
    context = InitHermesCore(&config, true);

    int fd = mkstemp(fname_template);
    if (fd == -1) {
      perror("mkstemp failed");
    } else {
      file_handle = fdopen(fd, "w");
      SerializeBufferPoolToFile(&context, file_handle);
      fflush(file_handle);
    }
    unlink(fname_template);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Bcast(fname_template, sizeof(fname_template), MPI_CHAR, 0,
            MPI_COMM_WORLD);

  if (world_rank != 0) {
    file_handle = fopen(fname_template, "r");

    if (file_handle) {
      context = hermes::InitHermesClient(world_rank, false);

      int bytes_per_rank = context.shm_size / (world_size - 1);
      int remaining_bytes = context.shm_size % (world_size - 1);

      if (world_rank == world_size - 1) {
        bytes_per_rank += remaining_bytes;
      }

      std::shared_ptr<hapi::Hermes> hstate = std::make_shared<hapi::Hermes>();
      hapi::Context ctx;
      hapi::Bucket bucket(std::string("test_bucket"), hstate);

      hapi::Blob put_data;
      put_data.reserve(bytes_per_rank);

      long starting_offset;
      if (world_rank == world_size - 1) {
        starting_offset = world_rank * (bytes_per_rank - remaining_bytes);
      } else {
        starting_offset = world_rank * bytes_per_rank;
      }

      fseek(file_handle, starting_offset, SEEK_SET);
      fread(put_data.data(), bytes_per_rank, 1, file_handle);

      std::string blob_name = ("serialzied_buffer_pool_" +
                               std::to_string(world_rank));
      bucket.Put(blob_name, put_data, ctx);

      hapi::Blob get_result;
      size_t blob_size = bucket.Get("", get_result, ctx);
      get_result.reserve(blob_size);
      blob_size = bucket.Get("", get_result, ctx);

      bucket.Release(ctx);
    } else {
      perror("Couldn't open file");
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  fclose(file_handle);
  MPI_Finalize();

  return 0;
}
