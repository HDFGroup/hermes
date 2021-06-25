/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include <stdio.h>
#include <unistd.h>

#include <chrono>

#include <mpi.h>

#include "hermes.h"
#include "utils.h"
#include "metadata_management_internal.h"
#include "test_utils.h"

namespace hapi = hermes::api;
using std::chrono::time_point;
const auto now = std::chrono::high_resolution_clock::now;
const int kNumRequests = 512;
constexpr int sizeof_id = sizeof(hermes::BufferID);

struct Options {
  bool bench_local;
  bool bench_remote;
  bool bench_server_scalability;
  char *config_file;
};

double GetAvgSeconds(time_point<std::chrono::high_resolution_clock> start,
                     time_point<std::chrono::high_resolution_clock> end,
                     hapi::Hermes *hermes, MPI_Comm comm) {
  double local_seconds = std::chrono::duration<double>(end - start).count();
  double total_seconds = 0;
  MPI_Reduce(&local_seconds, &total_seconds, 1, MPI_DOUBLE, MPI_SUM, 0, comm);
  double avg_seconds = total_seconds / hermes->GetNumProcesses();

  return avg_seconds;
}

void Run(int target_node, int rank, int comm_size, int num_requests,
         MPI_Comm comm, hapi::Hermes *hermes) {
  for (hermes::u32 num_bytes = 8;
       num_bytes <= KILOBYTES(4);
       num_bytes *= 2) {
    int num_ids = num_bytes / sizeof_id;

    std::vector<hermes::BufferID> buffer_ids(num_ids);
    for (int i = 0; i < num_ids; ++i) {
      hermes::BufferID id = {};
      id.bits.node_id = target_node;
      id.bits.header_index = i;
      buffer_ids[i] = id;
    }

    hermes::u32 id_list_offset =
      hermes::AllocateBufferIdList(&hermes->context_, &hermes->rpc_,
                                   target_node, buffer_ids);

    MPI_Barrier(comm);

    hermes::BlobID blob_id = {};
    blob_id.bits.node_id = target_node;
    blob_id.bits.buffer_ids_offset = id_list_offset;

    time_point start_get = now();
    for (int j = 0; j < num_requests; ++j) {
      std::vector<hermes::BufferID> ids =
        hermes::GetBufferIdList(&hermes->context_, &hermes->rpc_, blob_id);
    }
    time_point end_get = now();
    MPI_Barrier(comm);

    FreeBufferIdList(&hermes->context_, &hermes->rpc_, blob_id);

    // TODO(chogan): Time 'put' and 'delete' once they are optimized. For now
    // they are too slow so we just time 'get'.
    double avg_get_seconds = GetAvgSeconds(start_get, end_get, hermes, comm);

    if (rank == 0) {
      printf("Hermes,%d,%d,%d,%f\n", comm_size, hermes->comm_.num_nodes,
             (int)num_bytes, num_requests / avg_get_seconds);
    }
  }
}

void BenchLocal() {
  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.capacities[0] = GIGABYTES(2);
  config.arena_percentages[hermes::kArenaType_BufferPool] = 0.15;
  config.arena_percentages[hermes::kArenaType_MetaData] = 0.74;

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(&config);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();
    int target_node = hermes->rpc_.node_id;

    MPI_Comm *comm = (MPI_Comm *)hermes->GetAppCommunicator();
    Run(target_node, app_rank, app_size, kNumRequests, *comm, hermes.get());
    hermes->AppBarrier();
  } else {
    // Hermes core. No user code.
  }
  hermes->Finalize();
}

void BenchRemote(const char *config_file) {
  std::shared_ptr<hapi::Hermes> hermes = hapi::InitHermes(config_file);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();

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

    if (hermes->rpc_.node_id != 1) {
      const int kTargetNode = 1;
      Run(kTargetNode, client_rank, client_comm_size, kNumRequests, client_comm,
          hermes.get());
    }
    hermes->AppBarrier();
  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();
}

void BenchServerScalability(const char *config_file) {
  std::shared_ptr<hapi::Hermes> hermes = hapi::InitHermes(config_file);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();
    int num_nodes = hermes->comm_.num_nodes;
    // NOTE(chogan): A node_id of 0 is the NULL node, so we add 1.
    int target_node = (app_rank % num_nodes) + 1;

    if (app_rank == 0) {
      printf("Library,Clients,Servers,BytesTransferred,Ops/sec\n");
    }

    MPI_Comm *comm = (MPI_Comm *)hermes->GetAppCommunicator();
    Run(target_node, app_rank, app_size, kNumRequests, *comm, hermes.get());
    hermes->AppBarrier();
  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();
}

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s -[rsx] [-f config_file]\n", program);
  fprintf(stderr, "  -f\n");
  fprintf(stderr, "     Name of configuration file.\n");
  fprintf(stderr, "  -r\n");
  fprintf(stderr, "     Bench remote operations only.\n");
  fprintf(stderr, "  -s\n");
  fprintf(stderr, "     Bench local performance on a single node.\n");
  fprintf(stderr, "  -x\n");
  fprintf(stderr, "     Bench server scalability.\n");
}

Options HandleArgs(int argc, char **argv) {
  Options result = {};
  int option = -1;

  while ((option = getopt(argc, argv, "f:rsx")) != -1) {
    switch (option) {
      case 'f': {
        result.config_file = optarg;
        break;
      }
      case 'r': {
        result.bench_remote = true;
        break;
      }
      case 's': {
        result.bench_local = true;
        break;
      }
      case 'x': {
        result.bench_server_scalability = true;
        break;
      }
      default:
        PrintUsage(argv[0]);
        exit(1);
    }
  }

  if ((result.bench_remote || result.bench_server_scalability) &&
      !result.config_file) {
    fprintf(stderr, "Remote configuration option -r requires a config file name"
            "with the -f option.\n");
    exit(1);
  }

  if (optind < argc) {
    fprintf(stderr, "non-option ARGV-elements: ");
    while (optind < argc) {
      fprintf(stderr, "%s ", argv[optind++]);
    }
    fprintf(stderr, "\n");
  }

  return result;
}

int main(int argc, char **argv) {
  Options opts = HandleArgs(argc, argv);
  hermes::testing::InitMpi(&argc, &argv);

  if (opts.bench_local) {
    BenchLocal();
  }
  if (opts.bench_remote) {
    BenchRemote(opts.config_file);
  }
  if (opts.bench_server_scalability) {
    BenchServerScalability(opts.config_file);
  }

  MPI_Finalize();

  return 0;
}
