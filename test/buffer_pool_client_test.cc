#include <sys/mman.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <utility>

#include <mpi.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

#include "common.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "utils.h"

/**
 * @file buffer_pool_client_test.cc
 *
 * This shows how an application core might interact with the BufferPool. A
 * client must open the existing BufferPool shared memory (created by the hermes
 * core intialization), intialize communication, set up files for buffering, and
 * can then call the BufferPool either directly (same node) or via RPC (remote
 * node).
 */

using namespace hermes;

struct TimingResult {
  double get_buffers_time;
  double release_buffers_time;
};

TimingResult TestGetBuffersRpc(RpcContext *rpc, int iters) {
  TimingResult result = {};
  TieredSchema schema{std::make_pair(4096, 0)};

  Timer get_timer;
  Timer release_timer;
  for (int i = 0; i < iters; ++i) {
    get_timer.resumeTime();
    std::vector<BufferID> ret =
      RpcCall<std::vector<BufferID>>(rpc, rpc->node_id, "GetBuffers", schema);
    get_timer.pauseTime();

    if (ret.size() == 0) {
      break;
    }

    release_timer.resumeTime();
    for (size_t i = 0; i < ret.size(); ++i) {
      RpcCall<void>(rpc, rpc->node_id, "RemoteReleaseBuffer", ret[i]);
    }
    release_timer.pauseTime();
  }

  result.get_buffers_time = get_timer.getElapsedTime();
  result.release_buffers_time = release_timer.getElapsedTime();

  return result;
}

TimingResult TestGetBuffers(SharedMemoryContext *context, int iters) {
  TimingResult result = {};
  TieredSchema schema{std::make_pair(4096, 0)};

  Timer get_timer;
  Timer release_timer;
  for (int i = 0; i < iters; ++i) {
    get_timer.resumeTime();
    std::vector<BufferID> ret = GetBuffers(context, schema);
    get_timer.pauseTime();

    if (ret.size() == 0) {
      break;
    }

    release_timer.resumeTime();
    LocalReleaseBuffers(context, ret);
    release_timer.pauseTime();
  }

  result.get_buffers_time = get_timer.getElapsedTime();
  result.release_buffers_time = release_timer.getElapsedTime();

  return result;
}

double TestSplitBuffers(SharedMemoryContext *context, RpcContext *rpc,
                        int slab_index, bool use_rpc) {
  Timer timer;
  if (use_rpc) {
    timer.resumeTime();
    RpcCall<void>(rpc, rpc->node_id, "SplitBuffers", slab_index);
    timer.pauseTime();
  } else {
    timer.resumeTime();
    SplitRamBufferFreeList(context, slab_index);
    timer.pauseTime();
  }
  double result = timer.getElapsedTime();
  // TODO(chogan): Verify results
  return result;
}

double TestMergeBuffers(SharedMemoryContext *context, RpcContext *rpc,
                        int slab_index, bool use_rpc) {
  Timer timer;

  if (use_rpc) {
    timer.resumeTime();
    RpcCall<void>(rpc, rpc->node_id, "MergeBuffers", slab_index);
    timer.pauseTime();
  } else {
    timer.resumeTime();
    MergeRamBufferFreeList(context, slab_index);
    timer.pauseTime();
  }
  double result = timer.getElapsedTime();
  // TODO(chogan): Verify results
  return result;
}

struct FileMapper {

  Blob blob;

  FileMapper(const char *path) {
    FILE *f = fopen(path, "r");
    if (f) {
      fseek(f, 0, SEEK_END);
      blob.size = ftell(f);
      blob.data =  (u8 *)mmap(0, blob.size, PROT_READ, MAP_PRIVATE, fileno(f),
                              0);
      fclose(f);
    } else {
      perror(0);
    }
  }

  ~FileMapper() {
    munmap(blob.data, blob.size);
  }
};

void TestFileBuffering(SharedMemoryContext *context, RpcContext *rpc,
                       int rank, const char *test_file) {
  TierID tier_id = 1;

  FileMapper mapper(test_file);
  Blob blob = mapper.blob;

  if (blob.data) {
    TieredSchema schema{std::make_pair(blob.size, tier_id)};
    std::vector<BufferID> buffer_ids(0);

    while (buffer_ids.size() == 0) {
      buffer_ids = GetBuffers(context, schema);
    }

    WriteBlobToBuffers(context, rpc, blob, buffer_ids);

    std::vector<u8> data(blob.size);
    Blob result = {};
    result.size = blob.size;
    result.data = data.data();

    BufferIdArray buffer_id_arr = {};
    buffer_id_arr.ids = buffer_ids.data();
    buffer_id_arr.length = buffer_ids.size();
    ReadBlobFromBuffers(context,  rpc, &result, &buffer_id_arr);

    std::stringstream out_filename_stream;
    out_filename_stream << "TestfileBuffering_rank" << std::to_string(rank)
                        << ".bmp";
    std::string out_filename = out_filename_stream.str();
    FILE *out_file = fopen(out_filename.c_str(), "w");
    size_t items_written = fwrite(result.data, result.size, 1, out_file);
    Assert(items_written == 1);
    fclose(out_file);

    LocalReleaseBuffers(context, buffer_ids);

    FileMapper result_mapper(out_filename.c_str());
    Blob result_blob = result_mapper.blob;
    Assert(blob.size == result_blob.size);
    Assert(memcmp(blob.data, result_blob.data, blob.size) == 0);
  } else {
    perror(0);
  }
}

void PrintUsage(char *program) {
  fprintf(stderr, "Usage %s [-r]\n", program);
  fprintf(stderr, "  -b\n");
  fprintf(stderr, "     Run GetBuffers test.\n");
  fprintf(stderr, "  -c <path>\n");
  fprintf(stderr, "     Path to a Hermes configuration file.\n");
  fprintf(stderr, "  -f <path>\n");
  fprintf(stderr, "     Run FileBuffering test on file at <path>.\n");
  fprintf(stderr, "  -h\n");
  fprintf(stderr, "     Print help message and exit.\n");
  fprintf(stderr, "  -i <num>\n");
  fprintf(stderr, "     Use <num> iterations in GetBuffers test.\n");
  fprintf(stderr, "  -k\n");
  fprintf(stderr, "     Kill RPC server on program exit.\n");
  fprintf(stderr, "  -m <num>\n");
  fprintf(stderr, "     Run MergeBuffers test on slab <num>.\n");
  fprintf(stderr, "  -r\n");
  fprintf(stderr, "     Run GetBuffers test using RPC.\n");
  fprintf(stderr, "  -s <num>\n");
  fprintf(stderr, "     Run SplitBuffers test on slab <num>.\n");
}

namespace hapi = hermes::api;

int main(int argc, char **argv) {
  int option = -1;
  bool test_get_release = false;
  char *config_file = 0;
  bool use_rpc = false;
  bool test_split = false;
  bool test_merge = false;
  bool test_file_buffering = false;
  char *test_file = 0;
  bool kill_server = false;
  int slab_index = 0;
  int iters = 100000;

  while ((option = getopt(argc, argv, "bc:f:hi:km:rs:")) != -1) {
    switch (option) {
      case 'b': {
        test_get_release = true;
        break;
      }
      case 'c': {
        config_file = optarg;
        break;
      }
      case 'f': {
        test_file_buffering = true;
        test_get_release = false;
        test_file = optarg;
        break;
      }
      case 'h': {
        PrintUsage(argv[0]);
        return 1;
      }
      case 'i': {
        iters = atoi(optarg);
        break;
      }
      case 'k': {
        kill_server = true;
        break;
      }
      case 'm': {
        test_merge = true;
        slab_index = atoi(optarg) - 1;
        test_get_release = false;
        break;
      }
      case 'r': {
        use_rpc = true;
        break;
      }
      case 's': {
        test_split = true;
        slab_index = atoi(optarg) - 1;
        test_get_release = false;
        break;
      }
      default:
        PrintUsage(argv[0]);
        return 1;
    }
  }

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermesClient(config_file);
  int app_rank = hermes->GetProcessRank();
  int app_size = hermes->GetNumProcesses();
  SharedMemoryContext *context = &hermes->context_;
  RpcContext *rpc = &hermes->rpc_;

  if (test_get_release) {
    TimingResult timing = {};

    if (use_rpc) {
      timing = TestGetBuffersRpc(rpc, iters);
    } else {
      timing = TestGetBuffers(context, iters);
    }

    int total_iters = iters * app_size;
    double total_get_seconds = 0;
    double total_release_seconds = 0;
    MPI_Reduce(&timing.get_buffers_time, &total_get_seconds, 1, MPI_DOUBLE,
               MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&timing.release_buffers_time, &total_release_seconds, 1,
               MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (app_rank == 0) {
      double avg_get_seconds = total_get_seconds / app_size;
      double avg_release_seconds = total_release_seconds / app_size;
      double gets_per_second = total_iters / avg_get_seconds;
      double releases_per_second = total_iters / avg_release_seconds;
      printf("%f %f ", gets_per_second, releases_per_second);
    }
  }

  if (test_split) {
    assert(app_size == 1);
    double seconds_for_split = TestSplitBuffers(context, rpc, slab_index,
                                                use_rpc);
    printf("%f\n", seconds_for_split);
  }
  if (test_merge) {
    assert(app_size == 1);
    double seconds_for_merge = TestMergeBuffers(context, rpc, slab_index,
                                                use_rpc);
    printf("%f\n", seconds_for_merge);
  }
  if (test_file_buffering) {
    Assert(test_file);
    TestFileBuffering(context, rpc, app_rank, test_file);
  }

  if (app_rank == 0 && kill_server) {
    hermes::RpcCall<void>(rpc, rpc->node_id, "RemoteFinalize");
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
