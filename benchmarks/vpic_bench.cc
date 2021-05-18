#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <numeric>
#include <thread>

#include "mpi.h"
#include "hermes.h"
#include "bucket.h"
#include "test_utils.h"

namespace hapi = hermes::api;
using u8 = hermes::u8;
using std::chrono::duration;
const auto now = std::chrono::high_resolution_clock::now;

const int kNumVariables = 8;
const int kDefaultIterations = 16;
const int kXdim = 64;
const int kYdim = 64;
const int kZdim = 64;


struct Options {
  size_t data_size_mb;
  const char *output_path;
  int num_iterations;
  int num_nodes;
  std::string dpe_policy;
  std::string config_path;
  bool shared_bucket;
  bool do_posix_io;
  bool direct_io;
  bool sync;
};

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s [-bhiopx] \n", program);
  fprintf(stderr, "  -c <hermes.conf_path>\n");
  fprintf(stderr, "     Path to a Hermes configuration file.\n");
  fprintf(stderr, "  -d (default false)\n");
  fprintf(stderr, "     Boolean flag. Do POSIX I/O with O_DIRECT.\n");
  fprintf(stderr, "  -f (default false)\n");
  fprintf(stderr, "     Boolean flag. fflush and fsync after every write\n");
  fprintf(stderr, "  -h\n");
  fprintf(stderr, "     Print help\n");
  fprintf(stderr, "  -i <num_iterations> (default %d)\n", kDefaultIterations);
  fprintf(stderr, "     The number of times to run the VPIC I/O kernel.\n");
  fprintf(stderr, "  -n <num_nodes> (default 1)\n");
  fprintf(stderr, "     The number of nodes (only required when -x is used)\n");
  fprintf(stderr, "  -o <output_file> (default ./)\n");
  fprintf(stderr, "     The path to an output file, which will be called\n"
                  "     'vpic_<rank>.out\n");
  fprintf(stderr, "  -p <data_size> (default 8)\n");
  fprintf(stderr, "     The size of particle data in MiB for each variable.\n");
  fprintf(stderr, "  -s (default true)\n");
  fprintf(stderr, "     Boolean flag. Whether to share a single Bucket or \n"
                  "     give each rank its own Bucket\n");
  fprintf(stderr, "  -x (default false)\n");
  fprintf(stderr, "     Boolean flag. If enabled, POSIX I/O is performed\n"
                  "     instead of going through Hermes.\n");
}

Options HandleArgs(int argc, char **argv) {
  Options result = {};
  result.data_size_mb = 8;
  result.output_path = "./";
  result.num_iterations = kDefaultIterations;
  result.num_nodes = 1;
  result.shared_bucket = true;
  result.dpe_policy = "none";

  int option = -1;
  while ((option = getopt(argc, argv, "c:dfhi:n:o:p:sx")) != -1) {
    switch (option) {
      case 'c': {
        result.config_path = optarg;
        break;
      }
      case 'd': {
        result.direct_io = true;
        break;
      }
      case 'f': {
        result.sync = true;
        break;
      }
      case 'h': {
        PrintUsage(argv[0]);
        exit(0);
      }
      case 'i': {
        result.num_iterations = atoi(optarg);
        break;
      }
      case 'n': {
        result.num_nodes = atoi(optarg);
        break;
      }
      case 'o': {
        result.output_path = optarg;
        break;
      }
      case 'p': {
        result.data_size_mb = (size_t)std::stoull(optarg);
        break;
      }
      case 's': {
        result.shared_bucket = false;
        break;
      }
      case 'x': {
        result.do_posix_io = true;
        break;
      }
      default: {
        PrintUsage(argv[0]);
        exit(1);
      }
    }
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

static inline double uniform_random_number() {
  return (((double)rand())/((double)(RAND_MAX)));
}

// template<typename T>
// static void DoFwrite(const std::vector<T> &vec, FILE *f) {
//   size_t total_bytes = vec.size() * sizeof(T);
//   size_t bytes_written = fwrite(vec.data(), 1, total_bytes, f);
//   CHECK_EQ(bytes_written, total_bytes);
// }

static void DoFwrite(float *data, size_t size, FILE *f) {
  size_t bytes_written = fwrite(data, 1, size, f);
  CHECK_EQ(bytes_written, size);
}

// template<typename T>
// static void DoWrite(const std::vector<T> &vec, int fd) {
//   size_t total_bytes = vec.size() * sizeof(T);
//   ssize_t bytes_written = write(fd, vec.data(), total_bytes);
//   CHECK_EQ(bytes_written, total_bytes);
// }

static void DoWrite(float *data, size_t size, int fd) {
  ssize_t bytes_written = write(fd, data, size);
  CHECK_EQ(bytes_written, size);
}
#if 0
static void InitParticles(const int x_dim, const int y_dim, const int z_dim,
                          std::vector<int> &id1, std::vector<int> &id2,
                          std::vector<float> &x, std::vector<float> &y,
                          std::vector<float> &z, std::vector<float> &px,
                          std::vector<float> &py, std::vector<float> &pz) {
  size_t num_particles = id1.size();

  std::iota(id1.begin(), id1.end(), 0);

  for (size_t i = 0; i < num_particles; ++i) {
    id2[i] = id1[i] * 2;
  }

  std::generate(x.begin(), x.end(), [&x_dim]() {
    return uniform_random_number() * x_dim;
  });

  std::generate(y.begin(), y.end(), [&y_dim]() {
    return uniform_random_number() * y_dim;
  });

  for (size_t i = 0; i < num_particles; ++i) {
    z[i] = ((double)id1[i] / num_particles) * z_dim;
  }

  std::generate(px.begin(), px.end(), [&x_dim]() {
    return uniform_random_number() * x_dim;
  });

  std::generate(py.begin(), py.end(), [&y_dim]() {
    return uniform_random_number() * y_dim;
  });

  for (size_t i = 0; i < num_particles; ++i) {
    pz[i] = ((double)id2[i] / num_particles) * z_dim;
  }
}
#endif

double GetMPIAverage(double rank_seconds, int num_ranks, MPI_Comm comm) {
  double total_secs = 0;
  MPI_Reduce(&rank_seconds, &total_secs, 1, MPI_DOUBLE, MPI_SUM, 0, comm);
  double result = total_secs / num_ranks;

  return result;
}

double GetMPIAverage(double rank_seconds, int num_ranks, MPI_Comm *comm) {
  double result = GetMPIAverage(rank_seconds, num_ranks, *comm);

  return result;
}

double GetBandwidth(const Options &options, double total_elapsed,
                    MPI_Comm comm) {
  double avg_total_seconds = GetMPIAverage(total_elapsed, 8, comm);
  double total_mb =
    options.data_size_mb * kNumVariables * options.num_iterations;
  double result = total_mb / avg_total_seconds;

  return result;
}

void PrintResults(const Options &options, double bandwidth,
                  const std::string &buffering) {
  int num_buckets = options.shared_bucket ? 1 : kNumVariables;
  printf("%s,%s,%d,%d,%d,%zu,%f\n", buffering.c_str(),
         options.dpe_policy.c_str(), options.num_nodes, num_buckets,
         options.num_iterations, options.data_size_mb, bandwidth);
}

void RunHermesBench(Options &options, float *data) {
  double bandwidth = 0;
  std::shared_ptr<hapi::Hermes> hermes = nullptr;

  if (options.config_path.size() != 0) {
    hermes = hapi::InitHermes(options.config_path.c_str());
  } else {
    hermes::Config config = {};
    hermes::InitDefaultConfig(&config);
    config.num_devices = 1;
    config.num_targets = 1;
    config.capacities[0] = 128 * 1024 * 1024;
    config.system_view_state_update_interval_ms = 500;

    hermes = hermes::InitHermes(&config);
  }

  if (hermes->IsApplicationCore()) {
    int my_rank = hermes->GetProcessRank();
    MPI_Comm *comm = (MPI_Comm *)hermes->GetAppCommunicator();
    options.num_nodes = hermes->rpc_.num_nodes;

    std::string bucket_name = "vpic_";

    if (options.shared_bucket) {
      bucket_name += "shared";
    } else {
      bucket_name += std::to_string(my_rank);
    }

    std::string blob_name = "data_" + std::to_string(my_rank);
    size_t total_bytes = MEGABYTES(options.data_size_mb);

    constexpr int kNumPolicies = 3;

    const std::array<hapi::PlacementPolicy, kNumPolicies> policies = {
      hapi::PlacementPolicy::kRandom,
      hapi::PlacementPolicy::kRoundRobin,
      hapi::PlacementPolicy::kMinimizeIoTime
    };

    const std::array<std::string, kNumPolicies> policy_strings = {
      "random",
      "round_robin",
      "minimize_io_time"
    };

    for (int p = 0; p < kNumPolicies; ++p) {
      hermes::testing::Timer timer;
      hapi::Context ctx;
      ctx.policy = policies[p];
      options.dpe_policy = policy_strings[p];

      hapi::Bucket bucket(bucket_name, hermes, ctx);

      hermes->AppBarrier();

      for (int i = 0; i < options.num_iterations; ++i) {
        timer.resumeTime();
        CHECK(bucket.Put(blob_name, (u8*)data, total_bytes).Succeeded());
        timer.pauseTime();

        // NOTE(chogan): Simulate computation and let SystemViewState update
        std::this_thread::sleep_for(std::chrono::duration<hermes::i64,
                                    std::milli>(500));

        // TODO(chogan): Investigate crash when these barriers aren't here
        hermes->AppBarrier();
        CHECK(bucket.DeleteBlob(blob_name).Succeeded());
        hermes->AppBarrier();
      }

      hermes->AppBarrier();

      if (options.shared_bucket) {
        if (my_rank != 0) {
          bucket.Release();
        }
        hermes->AppBarrier();
        if (my_rank == 0) {
          bucket.Destroy();
        }
      } else {
        // TODO(chogan): Investigate whey refcount is sometimes > 1
        bucket.Destroy();
      }

      bandwidth = GetBandwidth(options, timer.getElapsedTime(), *comm);

      if (my_rank == 0) {
        std::string buffering = "hermes";
        PrintResults(options, bandwidth, buffering);
      }
      hermes->AppBarrier();
    }
  } else {
    // Hermes core. No user code.
  }

  hermes->Finalize();
}

void RunPosixBench(Options &options, float *x, int rank) {
  hermes::testing::Timer timer;

  for (int i = 0; i < options.num_iterations; ++i) {
    std::string output_file = "vpic_posix_" + std::to_string(rank) + ".out";
    std::string output_path = (options.output_path + std::string("/") +
                               output_file);

    if (options.direct_io) {
      int fd = open(output_path.c_str(),
                    O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_SYNC,
                    S_IRUSR | S_IWUSR);
      CHECK_GT(fd, 0);
      timer.resumeTime();
      DoWrite(x, MEGABYTES(options.data_size_mb), fd);
      timer.pauseTime();
      CHECK_EQ(close(fd), 0);
    } else {
      FILE *f = fopen(output_path.c_str(), "w");
      CHECK(f);

      timer.resumeTime();
      DoFwrite(x, MEGABYTES(options.data_size_mb), f);

      if (options.sync) {
        CHECK_EQ(fflush(f), 0);
        CHECK_EQ(fsync(fileno(f)), 0);
      }
      timer.pauseTime();

      CHECK_EQ(fclose(f), 0);
    }
  }

  double bandwidth = GetBandwidth(options, timer.getElapsedTime(),
                                  MPI_COMM_WORLD);

  if (rank == 0) {
    std::string buffering = "normal";
    if (options.direct_io) {
      buffering = "direct";
    } else if (options.sync) {
      buffering = "sync";
    }
    PrintResults(options, bandwidth, buffering);
  }
}

int main(int argc, char* argv[]) {
  Options options = HandleArgs(argc, argv);

  MPI_Init(&argc, &argv);

  int rank = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0) {
    // TODO(chogan): DPE strategy, num Tiers, tier percentage
    // printf("posix,num_buckets,num_iterations,num_particles,write_seconds,"
    //        "flush_del_seconds\n");
  }

  size_t num_elements = MEGABYTES(options.data_size_mb) / sizeof(float);
  // std::vector<float> data(num_elements);
  const int kSectorSize = 512;
  float *data = (float *)aligned_alloc(kSectorSize,
                                       num_elements * sizeof(float));

  for (size_t i = 0; i < num_elements; ++i) {
    data[i] = uniform_random_number() * kXdim;
  }

  if (options.do_posix_io) {
    RunPosixBench(options, data, rank);
  } else {
    RunHermesBench(options, data);
  }

  free(data);

  MPI_Finalize();

  return 0;
}
