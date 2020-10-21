#include <stdio.h>
#include <unistd.h>

#include <chrono>

#include <mpi.h>

#include "hermes.h"
#include "utils.h"

namespace hapi = hermes::api;
using std::chrono::time_point;
const auto now = std::chrono::high_resolution_clock::now;
const int kNumRequests = 1024;
constexpr int sizeof_id = sizeof(hermes::BufferID);

double GetAvgSeconds(time_point<std::chrono::high_resolution_clock> start,
                     time_point<std::chrono::high_resolution_clock> end,
                     hapi::Hermes *hermes) {
  double local_seconds = std::chrono::duration<double>(end - start).count();
  double total_seconds = 0;
  MPI_Comm *app_comm = (MPI_Comm *)hermes->GetAppCommunicator();
  MPI_Reduce(&local_seconds, &total_seconds, 1, MPI_DOUBLE, MPI_SUM, 0,
             *app_comm);
  double avg_seconds = total_seconds / hermes->GetNumProcesses();

  return avg_seconds;
}

void BenchLocal() {
  const int kTargetNode = 1;

  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.capacities[0] = GIGABYTES(2);
  config.arena_percentages[hermes::kArenaType_BufferPool] = 0.15;
  config.arena_percentages[hermes::kArenaType_MetaData] = 0.74;

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(&config);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    hermes::MetadataManager *mdm =
      GetMetadataManagerFromContext(&hermes->context_);

    for (hermes::u32 num_bytes = 8;
         num_bytes <= KILOBYTES(4);
         num_bytes *= 2) {
      int num_ids = num_bytes / sizeof_id;

      std::vector<hermes::BufferID> buffer_ids(num_ids);
      for (hermes::u32 i = 0; i < num_ids; ++i) {
        hermes::BufferID id = {};
        // NOTE(chogan): The target node doesn't matter, since everything is
        // local
        id.bits.node_id = kTargetNode;
        id.bits.header_index = i;
        buffer_ids[i] = id;
      }

      hermes::u32 id_list_offsets[kNumRequests] = {0};

      time_point start_put = now();
      for (int j = 0; j < kNumRequests; ++j) {
        id_list_offsets[j] = LocalAllocateBufferIdList(mdm, buffer_ids);
      }
      time_point end_put = now();

      hermes->AppBarrier();

      hermes::BlobID blob_id = {};
      blob_id.bits.node_id = app_rank;

      hermes::BufferIdArray buffer_id_arr = {};

      time_point start_get = now();
      for (int j = 0; j < kNumRequests; ++j) {
        {
          hermes::ScopedTemporaryMemory scratch(&hermes->trans_arena_);
          blob_id.bits.buffer_ids_offset = id_list_offsets[j];
          hermes::LocalGetBufferIdList(scratch, mdm, blob_id, &buffer_id_arr);
        }
      }
      time_point end_get = now();

      hermes->AppBarrier();

      time_point start_del = now();
      for (int j = 0; j < kNumRequests; ++j) {
        blob_id.bits.buffer_ids_offset = id_list_offsets[j];
        LocalFreeBufferIdList(&hermes->context_, blob_id);
      }
      time_point end_del = now();

      hermes->AppBarrier();

      double avg_put_seconds = GetAvgSeconds(start_put, end_put, hermes.get());
      double avg_get_seconds = GetAvgSeconds(start_get, end_get, hermes.get());
      double avg_del_seconds = GetAvgSeconds(start_del, end_del, hermes.get());

      if (hermes->IsFirstRankOnNode()) {
        printf("put,%d,%d,%f\n", app_size, (int)num_bytes,
               kNumRequests / avg_put_seconds);
        printf("get,%d,%d,%f\n", app_size, num_bytes,
               kNumRequests / avg_get_seconds);
        printf("del,%d,%d,%f\n", app_size, num_bytes,
               kNumRequests / avg_del_seconds);
      }
    }

  } else {
    // Hermes core. No user code.
  }
  hermes->Finalize();
}

void BenchRemote(const char *config_file) {
  const int kTargetNode = 1;

  std::shared_ptr<hapi::Hermes> hermes = hapi::InitHermes(config_file);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    MPI_Comm *app_comm = (MPI_Comm *)hermes->GetAppCommunicator();

    // NOTE(chogan): Create a communicator for app ranks not on node 1 so that
    // all requests are remote.
    MPI_Comm client_comm;
    MPI_Comm_split(*app_comm, hermes->rpc_.node_id != 1, app_rank,
                   &client_comm);
    int client_comm_size;
    int client_rank;
    MPI_Comm_size(client_comm, &client_comm_size);
    MPI_Comm_rank(client_comm, &client_rank);
    printf("client_comm size: %d\n", client_comm_size);
    printf("app_comm size: %d\n", app_size);

    if (hermes->rpc_.node_id != 1) {
      for (hermes::u32 num_bytes = 8;
           num_bytes <= KILOBYTES(4);
           num_bytes *= 2) {
        int num_ids = num_bytes / sizeof_id;

        std::vector<hermes::BufferID> buffer_ids(num_ids);
        for (hermes::u32 i = 0; i < num_ids; ++i) {
          hermes::BufferID id = {};
          id.bits.node_id = kTargetNode;
          id.bits.header_index = i;
          buffer_ids[i] = id;
        }

        hermes::u32 id_list_offsets[kNumRequests] = {0};

        time_point start_put = now();
        for (int j = 0; j < kNumRequests; ++j) {
          id_list_offsets[j] =
            hermes::AllocateBufferIdList(&hermes->context_, &hermes->rpc_,
                                         kTargetNode, buffer_ids);
        }
        time_point end_put = now();

        hermes::BlobID blob_id = {};
        blob_id.bits.node_id = kTargetNode;

        MPI_Barrier(client_comm);
        time_point start_get = now();
        for (int j = 0; j < kNumRequests; ++j) {
          blob_id.bits.buffer_ids_offset = id_list_offsets[j];
          std::vector<hermes::BufferID> ids =
            hermes::GetBufferIdList(&hermes->context_, &hermes->rpc_, blob_id);
        }
        time_point end_get = now();
        MPI_Barrier(client_comm);

        time_point start_del = now();
        for (int j = 0; j < kNumRequests; ++j) {
          blob_id.bits.buffer_ids_offset = id_list_offsets[j];
          FreeBufferIdList(&hermes->context_, &hermes->rpc_, blob_id);
        }
        time_point end_del = now();

        double put_seconds =
          std::chrono::duration<double>(end_put - start_put).count();
        double local_get_seconds =
          std::chrono::duration<double>(end_get - start_get).count();
        double del_seconds =
          std::chrono::duration<double>(end_del - start_del).count();

        double total_get_seconds;
        MPI_Reduce(&local_get_seconds, &total_get_seconds, 1,
                   MPI_DOUBLE, MPI_SUM, 0, client_comm);
        double avg_get_seconds = total_get_seconds / client_comm_size;

        // double payload_megabytes =
        //   (num_bytes * kNumRequests) / 1024.0 / 1024.0;
        // printf("put,%d,%d,%f,%f\n", client_comm_size, num_bytes,
        //        kNumRequests / put_seconds, payload_megabytes / put_seconds);
        if (client_rank == 0) {
          printf("%d,%d,%f\n", client_comm_size, (int)num_bytes,
                 kNumRequests / avg_get_seconds);
        }
        // printf("del,%d,%d,%f\n", client_comm_size, num_bytes,
        //        kNumRequests / del_seconds);
        // printf("put,remote,1,1,%d,%.8f\n", payload_bytes, put_seconds);
        // printf("get,local,1,%d,%d,%.8f\n", client_comm_size, payload_bytes,
        //        max_get_seconds);
        // printf("del,remote,1,1,%d,%.8f\n", payload_bytes, del_seconds);
      }
    }

    hermes->AppBarrier();

  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();
}

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s -[rs]\n", program);
  fprintf(stderr, "  -f\n");
  fprintf(stderr, "     Name of configuration file.\n");
  fprintf(stderr, "  -r\n");
  fprintf(stderr, "     Bench remote operations only.\n");
  fprintf(stderr, "  -s\n");
  fprintf(stderr, "     Bench local performance on a single node.\n");
}

int main(int argc, char **argv) {
  int option = -1;
  bool bench_local = false;
  bool bench_remote = false;
  char *config_file = 0;

  while ((option = getopt(argc, argv, "f:rs")) != -1) {
    switch (option) {
      case 'f': {
        config_file = optarg;
        break;
      }
      case 'r': {
        bench_remote = true;
        break;
      }
      case 's': {
        bench_local = true;
        break;
      }
      default:
        PrintUsage(argv[0]);
        return 1;
    }
  }

  if (bench_remote && !config_file) {
    fprintf(stderr, "Remote configuration option -r requires a config file name"
            "with the -f option.\n");
    return 1;
  }

  if (optind < argc) {
    fprintf(stderr, "non-option ARGV-elements: ");
    while (optind < argc) {
      fprintf(stderr, "%s ", argv[optind++]);
    }
    fprintf(stderr, "\n");
  }

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  if (bench_local) {
    BenchLocal();
  }
  if (bench_remote) {
    BenchRemote(config_file);
  }

  MPI_Finalize();

  return 0;
}
