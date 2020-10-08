#include <chrono>

#include <mpi.h>

#include "hermes.h"
#include "utils.h"

namespace hapi = hermes::api;

int main(int argc, char **argv) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  const auto now = std::chrono::high_resolution_clock::now;

  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(&config);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    hermes::MetadataManager *mdm =
      GetMetadataManagerFromContext(&hermes->context_);

    constexpr int sizeof_id = sizeof(hermes::BufferID);

    for (hermes::u32 i = 1; i <= KILOBYTES(4) / sizeof_id; i *= 2) {
      int num_ids = i;
      int payload_bytes = sizeof_id * num_ids;

      std::vector<hermes::BufferID> buffer_ids(i);
      for (hermes::u32 j = 0; j < i; ++j) {
        hermes::BufferID id = {};
        id.bits.node_id = app_rank;
        id.bits.header_index = j;
        buffer_ids[j] = id;
      }

      auto start = now();
      hermes::u32 location = LocalAllocateBufferIdList(mdm, buffer_ids);
      auto end = now();

      if (i == 1) {
        // NOTE(chogan): Prime the cache
        hermes::BlobID blob_id = {};
        blob_id.bits.node_id = app_rank;
        blob_id.bits.buffer_ids_offset = location;
        LocalFreeBufferIdList(&hermes->context_, blob_id);

        start = now();
        location = LocalAllocateBufferIdList(mdm, buffer_ids);
        end = now();
      }

      hermes::BlobID blob_id = {};
      blob_id.bits.node_id = app_rank;
      blob_id.bits.buffer_ids_offset = location;
      LocalFreeBufferIdList(&hermes->context_, blob_id);

      hermes->AppBarrier();

      double seconds = std::chrono::duration<double>(end - start).count();
      double max_seconds = 0;
      MPI_Comm *app_comm = (MPI_Comm *)hermes->GetAppCommunicator();
      MPI_Reduce(&seconds, &max_seconds, 1, MPI_DOUBLE, MPI_MAX, 0, *app_comm);

      if (hermes->comm_.first_on_node) {
        printf("local,1,%d,%d,%f\n", app_size, payload_bytes, max_seconds);
      }
    }

  } else {
    // Hermes core. No user code.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
