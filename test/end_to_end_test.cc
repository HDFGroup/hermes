#include <assert.h>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "test_utils.h"

namespace hapi = hermes::api;

void TestPutGetBucket(hapi::Bucket &bucket, int app_rank, int app_size) {
  size_t bytes_per_rank = KILOBYTES(8);
  if (app_size) {
    size_t data_size = KILOBYTES(8);
    bytes_per_rank = data_size / app_size;
    size_t remaining_bytes = data_size % app_size;

    if (app_rank == app_size - 1) {
      bytes_per_rank += remaining_bytes;
    }
  }

  hapi::Blob put_data(bytes_per_rank, rand() % 255);

  hapi::Context ctx;
  std::string blob_name = "test_blob" + std::to_string(app_rank);
  bucket.Put(blob_name, put_data, ctx);

  hapi::Blob get_result;
  size_t blob_size = bucket.Get(blob_name, get_result, ctx);
  get_result.resize(blob_size);
  blob_size = bucket.Get(blob_name, get_result, ctx);

  Assert(put_data == get_result);
}

void TestBulkTransfer(std::shared_ptr<hapi::Hermes> hermes, int app_rank) {
  size_t transfer_size = KILOBYTES(8);

  hapi::Context ctx;
  hapi::Bucket bucket(std::string("test_bucket"), hermes, ctx);
  std::string blob_name = "1";

  hapi::Blob put_data(transfer_size, 'x');

  if (app_rank == 0) {
    bucket.Put(blob_name, put_data, ctx);
  }

  hermes->AppBarrier();

  if (app_rank != 0) {
    hapi::Blob get_result;
    size_t blob_size = bucket.Get(blob_name, get_result, ctx);
    get_result.resize(blob_size);
    blob_size = bucket.Get(blob_name, get_result, ctx);

    Assert(get_result == put_data);

    bucket.Close(ctx);
  }

  hermes->AppBarrier();

  if (app_rank == 0) {
    bucket.Destroy(ctx);
  }
}

int main(int argc, char **argv) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }

  std::shared_ptr<hapi::Hermes> hermes = hapi::InitHermes(config_file);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    hapi::Context ctx;

    // Each rank puts and gets its portion of a blob to a shared bucket
    hapi::Bucket shared_bucket(std::string("test_bucket"), hermes, ctx);
    TestPutGetBucket(shared_bucket, app_rank, app_size);

    if (app_rank != 0) {
      shared_bucket.Close(ctx);
    }

    hermes->AppBarrier();

    if (app_rank == 0) {
      shared_bucket.Destroy(ctx);
    }

    hermes->AppBarrier();

    // Each rank puts a whole blob to its own bucket
    hapi::Bucket own_bucket(std::string("test_bucket_") +
                            std::to_string(app_rank), hermes, ctx);
    TestPutGetBucket(own_bucket, app_rank, 0);
    own_bucket.Destroy(ctx);

    TestBulkTransfer(hermes, app_rank);
  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
