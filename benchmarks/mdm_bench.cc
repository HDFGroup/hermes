#include <chrono>

#include <mpi.h>

#include "hermes.h"
#include "utils.h"

namespace hapi = hermes::api;
using std::chrono::time_point;

double GetMaxSeconds(time_point<std::chrono::high_resolution_clock> start,
                     time_point<std::chrono::high_resolution_clock> end,
                     hapi::Hermes *hermes) {
  double seconds = std::chrono::duration<double>(end - start).count();
  double max_seconds = 0;
  MPI_Comm *app_comm = (MPI_Comm *)hermes->GetAppCommunicator();
  MPI_Reduce(&seconds, &max_seconds, 1, MPI_DOUBLE, MPI_MAX, 0, *app_comm);

  return max_seconds;
}

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

      time_point start_put = now();
      hermes::u32 location = LocalAllocateBufferIdList(mdm, buffer_ids);
      time_point end_put = now();

      hermes::BlobID blob_id = {};
      blob_id.bits.node_id = app_rank;
      blob_id.bits.buffer_ids_offset = location;

      hermes::ScopedTemporaryMemory scratch(&hermes->trans_arena_);
      hermes::BufferIdArray buffer_id_arr = {};

      time_point start_get = now();
      hermes::LocalGetBufferIdList(scratch, mdm, blob_id, &buffer_id_arr);
      time_point end_get = now();

      time_point start_del = now();
      LocalFreeBufferIdList(&hermes->context_, blob_id);
      time_point end_del = now();

      hermes->AppBarrier();

      double max_put_seconds = GetMaxSeconds(start_put, end_put, hermes.get());
      double max_get_seconds = GetMaxSeconds(start_get, end_get, hermes.get());
      double max_del_seconds = GetMaxSeconds(start_del, end_del, hermes.get());

      if (hermes->comm_.first_on_node) {
        printf("put,local,1,%d,%d,%.8f\n", app_size, payload_bytes,
               max_put_seconds);
        printf("get,local,1,%d,%d,%.8f\n", app_size, payload_bytes,
               max_get_seconds);
        printf("del,local,1,%d,%d,%.8f\n", app_size, payload_bytes,
               max_del_seconds);
      }
    }

  } else {
    // Hermes core. No user code.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
