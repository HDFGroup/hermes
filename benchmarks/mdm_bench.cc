#include <stdio.h>
#include <unistd.h>

#include <chrono>

#include <mpi.h>

#include "hermes.h"
#include "utils.h"

namespace hapi = hermes::api;
using std::chrono::time_point;
const auto now = std::chrono::high_resolution_clock::now;

double GetMaxSeconds(time_point<std::chrono::high_resolution_clock> start,
                     time_point<std::chrono::high_resolution_clock> end,
                     hapi::Hermes *hermes) {
  double seconds = std::chrono::duration<double>(end - start).count();
  double max_seconds = 0;
  MPI_Comm *app_comm = (MPI_Comm *)hermes->GetAppCommunicator();
  MPI_Reduce(&seconds, &max_seconds, 1, MPI_DOUBLE, MPI_MAX, 0, *app_comm);

  return max_seconds;
}

void BenchLocal() {
  constexpr int sizeof_id = sizeof(hermes::BufferID);

  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.arena_percentages[hermes::kArenaType_BufferPool] = 0.55;
  config.arena_percentages[hermes::kArenaType_MetaData] = 0.34;

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(&config);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    hermes::MetadataManager *mdm =
      GetMetadataManagerFromContext(&hermes->context_);

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

      const int kRepeats = 64;
      hermes::u32 id_list_offsets[kRepeats] = {0};

      time_point start_put = now();
      for (int j = 0; j < kRepeats; ++j) {
        id_list_offsets[j] = LocalAllocateBufferIdList(mdm, buffer_ids);
      }
      time_point end_put = now();

      hermes::BlobID blob_id = {};
      blob_id.bits.node_id = app_rank;

      hermes::BufferIdArray buffer_id_arr = {};

      time_point start_get = now();
      for (int j = 0; j < kRepeats; ++j) {
        {
          hermes::ScopedTemporaryMemory scratch(&hermes->trans_arena_);
          blob_id.bits.buffer_ids_offset = id_list_offsets[j];
          hermes::LocalGetBufferIdList(scratch, mdm, blob_id, &buffer_id_arr);
        }
      }
      time_point end_get = now();

      time_point start_del = now();
      for (int j = 0; j < kRepeats; ++j) {
        blob_id.bits.buffer_ids_offset = id_list_offsets[j];
        LocalFreeBufferIdList(&hermes->context_, blob_id);
      }
      time_point end_del = now();

      hermes->AppBarrier();

      // TODO(chogan): Avg?
      double max_put_seconds = GetMaxSeconds(start_put, end_put, hermes.get());
      double max_get_seconds = GetMaxSeconds(start_get, end_get, hermes.get());
      double max_del_seconds = GetMaxSeconds(start_del, end_del, hermes.get());

      if (hermes->IsFirstRankOnNode()) {
        double payload_megabytes = (payload_bytes * kRepeats) / 1024.0 / 1024.0;
        printf("put,local,1,%d,%d,%f,%f\n", app_size, payload_bytes,
               kRepeats / max_put_seconds, payload_megabytes / max_put_seconds);
        printf("get,local,1,%d,%d,%f,%f\n", app_size, payload_bytes,
               kRepeats / max_get_seconds, payload_megabytes / max_get_seconds);
        printf("del,local,1,%d,%d,%f\n", app_size, payload_bytes,
                kRepeats / max_del_seconds);
      }
    }

  } else {
    // Hermes core. No user code.
  }
  hermes->Finalize();
}

void BenchRemote(const char *config_file) {
  constexpr int sizeof_id = sizeof(hermes::BufferID);

  std::shared_ptr<hapi::Hermes> hermes = hapi::InitHermes(config_file);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    if (app_rank == 0) {
      for (hermes::u32 i = 1; i <= KILOBYTES(4) / sizeof_id; i *= 2) {
        int num_ids = i;
        int payload_bytes = sizeof_id * num_ids;

        // NOTE(chogan): Send data to the next node.
        int target_node = hermes->GetNodeId() + 1;

        std::vector<hermes::BufferID> buffer_ids(i);
        for (hermes::u32 j = 0; j < i; ++j) {
          hermes::BufferID id = {};
          id.bits.node_id = target_node;
          id.bits.header_index = j;
          buffer_ids[j] = id;
        }

        const int kRepeats = 512;
        hermes::u32 id_list_offsets[kRepeats] = {0};

        time_point start_put = now();
        for (int j = 0; j < kRepeats; ++j) {
          id_list_offsets[j] =
            hermes::AllocateBufferIdList(&hermes->context_, &hermes->rpc_,
                                         target_node, buffer_ids);
        }
        time_point end_put = now();

        hermes::BlobID blob_id = {};
        blob_id.bits.node_id = target_node;

        time_point start_get = now();
        for (int j = 0; j < kRepeats; ++j) {
          blob_id.bits.buffer_ids_offset = id_list_offsets[j];
          std::vector<hermes::BufferID> ids =
            hermes::GetBufferIdList(&hermes->context_, &hermes->rpc_,blob_id);
        }
        time_point end_get = now();

        time_point start_del = now();
        for (int j = 0; j < kRepeats; ++j) {
          blob_id.bits.buffer_ids_offset = id_list_offsets[j];
          FreeBufferIdList(&hermes->context_, &hermes->rpc_, blob_id);
        }
        time_point end_del = now();

        double put_seconds =
          std::chrono::duration<double>(end_put - start_put).count();
        double get_seconds =
          std::chrono::duration<double>(end_get - start_get).count();
        double del_seconds =
          std::chrono::duration<double>(end_del - start_del).count();

        double payload_megabytes = (payload_bytes * kRepeats) / 1024.0 / 1024.0;
        printf("put,%d,%d,%f,%f\n", app_size, payload_bytes,
               kRepeats / put_seconds, payload_megabytes / put_seconds);
        printf("get,%d,%d,%f,%f\n", app_size, payload_bytes,
               kRepeats / get_seconds, payload_megabytes / get_seconds);
        printf("del,%d,%d,%f\n", app_size, payload_bytes,
               kRepeats / del_seconds);
        // printf("put,remote,1,1,%d,%.8f\n", payload_bytes, put_seconds);
        // printf("get,local,1,%d,%d,%.8f\n", app_size, payload_bytes,
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
