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

const int kXdim = 64;

const int kDefaultSleepSeconds = 0;
const size_t kDefaultDataSizeMB = 8;
const size_t kDefaultIoSizeMB = kDefaultDataSizeMB;
const char *kDefaultOutputPath = "./";
const int kDefaultIterations = 1;
const char *kDefaultDpePolicy = "none";
const bool kDefaultSharedBucket = true;
const bool kDefaultDoPosixIo = true;
const bool kDefaultDirectIo = false;
const bool kDefaultSync = false;
const bool kDefaultVerifyResults = false;

struct Options {
  size_t data_size_mb;
  size_t io_size_mb;
  int num_iterations;
  int sleep_seconds;
  std::string output_path;
  std::string dpe_policy;
  bool shared_bucket;
  bool do_posix_io;
  bool direct_io;
  bool sync;
  bool verify_results;
};

struct Timing {
  double fopen_time;
  double fwrite_time;
  double fclose_time;
  double compute_time;
  double total_time;
};

void GetDefaultOptions(Options *options) {
  options->sleep_seconds = kDefaultSleepSeconds;
  options->data_size_mb = kDefaultDataSizeMB;
  options->io_size_mb = kDefaultIoSizeMB;
  options->output_path = kDefaultOutputPath;
  options->num_iterations = kDefaultIterations;
  options->dpe_policy = kDefaultDpePolicy;
  options->shared_bucket = kDefaultSharedBucket;
  options->do_posix_io = kDefaultDoPosixIo;
  options->direct_io = kDefaultDirectIo;
  options->sync = kDefaultSync;
  options->verify_results = kDefaultVerifyResults;
}

#define HERMES_BOOL_OPTION(var) (var) ? "true" : "false"

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s [-bhiopx] \n", program);
  fprintf(stderr, "  -a <sleep_seconds> (default %d)\n", kDefaultSleepSeconds);
  fprintf(stderr, "     The number of seconds to sleep between each loop\n"
                  "     iteration to simulate computation.\n");
  fprintf(stderr, "  -c <hermes.conf_path>\n");
  fprintf(stderr, "     Path to a Hermes configuration file.\n");
  fprintf(stderr, "  -d (default %s)\n", HERMES_BOOL_OPTION(kDefaultDirectIo));
  fprintf(stderr, "     Boolean flag. Do POSIX I/O with O_DIRECT.\n");
  fprintf(stderr, "  -f (default %s)\n", HERMES_BOOL_OPTION(kDefaultSync));
  fprintf(stderr, "     Boolean flag. fflush and fsync after every write\n");
  fprintf(stderr, "  -h\n");
  fprintf(stderr, "     Print help\n");
  fprintf(stderr, "  -i <num_iterations> (default %d)\n", kDefaultIterations);
  fprintf(stderr, "     The number of times to run the VPIC I/O kernel.\n");
  fprintf(stderr, "     The number of nodes (only required when -x is used)\n");
  fprintf(stderr, "  -o <output_file> (default %s)\n", kDefaultOutputPath);
  fprintf(stderr, "     The path to an output file, which will be called\n"
                  "     'vpic_<rank>.out\n");
  fprintf(stderr, "  -p <data_size_mb> (default %zu)\n", kDefaultDataSizeMB);
  fprintf(stderr, "     The size of particle data in MiB for each variable.\n");
  fprintf(stderr, "  -s (default %s)\n",
          HERMES_BOOL_OPTION(kDefaultSharedBucket));
  fprintf(stderr, "     Boolean flag. Whether to share a single Bucket or \n"
                  "     give each rank its own Bucket\n");
  fprintf(stderr, "  -t <io_size_mb> (default is the value of -p)\n");
  fprintf(stderr, "     The size of each I/O. Must be a multiple of the total\n"
                  "     data_size_mb (-p). I/O will be done in a loop with\n"
                  "     io_size_mb/data_size_mb iterations\n");
  fprintf(stderr, "  -v (default %s)\n",
          HERMES_BOOL_OPTION(kDefaultVerifyResults));
  fprintf(stderr, "     Boolean flag. If enabled, read the written results\n"
                  "     and verify that they match what's expected.\n");
  fprintf(stderr, "  -x (default %s)\n", HERMES_BOOL_OPTION(kDefaultDoPosixIo));
  fprintf(stderr, "     Boolean flag. If enabled, POSIX I/O is performed\n"
                  "     instead of going through Hermes.\n");
}

Options HandleArgs(int argc, char **argv) {
  Options result = {};
  GetDefaultOptions(&result);

  bool io_size_provided = false;
  int option = -1;

  while ((option = getopt(argc, argv, "a:dfhi:o:p:st:vx")) != -1) {
    switch (option) {
      case 'a': {
        result.sleep_seconds = atoi(optarg);
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
      case 't': {
        result.io_size_mb = (size_t)std::stoull(optarg);
        io_size_provided = true;
        break;
      }
      case 'v': {
        result.verify_results = true;
        break;
      }
      case 'x': {
        result.do_posix_io = false;
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

  if (!io_size_provided) {
    result.io_size_mb = result.data_size_mb;
  }

  if (MEGABYTES(result.data_size_mb) % MEGABYTES(result.io_size_mb) != 0) {
    fprintf(stderr,
            "io_size_mb (-t) must be a multiple of data_size_mb (-p)\n");
    exit(1);
  }

  return result;
}

static inline double uniform_random_number() {
  return (((double)rand())/((double)(RAND_MAX)));
}

static void DoFwrite(void *data, size_t size, FILE *f) {
  size_t bytes_written = fwrite(data, 1, size, f);
  CHECK_EQ(bytes_written, size);
}

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

#if 0
double GetBandwidth(const Options &options, double total_elapsed,
                    MPI_Comm comm) {
  double avg_total_seconds = GetMPIAverage(total_elapsed, 8, comm);
  double total_mb =
    options.data_size_mb * kDefaultNumVariables * options.num_iterations;
  double result = total_mb / avg_total_seconds;

  return result;
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
#endif

void PrintResults(const Options &options, const Timing &timing,
                  double bandwidth) {
  double compute_percentage = timing.compute_time / timing.total_time;
  printf("%zu,%zu,%f,%f,%f,%f,%f,%f,%f\n", options.data_size_mb,
         options.io_size_mb, timing.fopen_time, timing.fwrite_time,
         timing.fclose_time, timing.total_time, timing.compute_time,
         bandwidth, compute_percentage);
}

std::string GetOutputPath(const std::string &output_path, int rank) {
  std::string output_file = "vpic_posix_" + std::to_string(rank) + ".out";
  int last_char_index = output_path.size() > 0 ? output_path.size() - 1 : 0;
  std::string maybe_slash = output_path[last_char_index] == '/' ? "" : "/";
  std::string result = output_path + maybe_slash + output_file;

  return result;
}

Timing RunPosixBench(Options &options, float *x, int rank) {
  Timing result = {};

  hermes::testing::Timer fopen_timer;
  hermes::testing::Timer fwrite_timer;
  hermes::testing::Timer fclose_timer;
  hermes::testing::Timer compute_timer;
  hermes::testing::Timer timer;

  for (int i = 0; i < options.num_iterations; ++i) {
    std::string output_path = GetOutputPath(options.output_path, rank);

    if (options.direct_io) {
      fopen_timer.resumeTime();
      int fd = open(output_path.c_str(),
                    O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_SYNC,
                    S_IRUSR | S_IWUSR);
      fopen_timer.pauseTime();
      MPI_Barrier(MPI_COMM_WORLD);

      CHECK_GT(fd, 0);
      fwrite_timer.resumeTime();
      DoWrite(x, MEGABYTES(options.data_size_mb), fd);
      fwrite_timer.pauseTime();
      MPI_Barrier(MPI_COMM_WORLD);

      fclose_timer.resumeTime();
      CHECK_EQ(close(fd), 0);
      fclose_timer.pauseTime();
      MPI_Barrier(MPI_COMM_WORLD);
    } else {
      fopen_timer.resumeTime();
      FILE *f = fopen(output_path.c_str(), "w");
      fopen_timer.pauseTime();
      CHECK(f);
      MPI_Barrier(MPI_COMM_WORLD);

      size_t num_ios = options.data_size_mb / options.io_size_mb;
      size_t byte_offset = 0;
      size_t byte_increment = MEGABYTES(options.data_size_mb) / num_ios;

      for (size_t iter = 0; iter < num_ios; ++iter) {
        auto sleep_seconds = std::chrono::seconds(options.sleep_seconds);
        compute_timer.resumeTime();
        std::this_thread::sleep_for(sleep_seconds);
        compute_timer.pauseTime();

        fwrite_timer.resumeTime();
        void *write_start = (hermes::u8 *)x + byte_offset;
        DoFwrite(write_start, byte_increment, f);

        if (options.sync) {
          CHECK_EQ(fflush(f), 0);
          CHECK_EQ(fsync(fileno(f)), 0);
        }
        fwrite_timer.pauseTime();
        byte_offset += byte_increment;
        MPI_Barrier(MPI_COMM_WORLD);
      }

      fclose_timer.resumeTime();
      CHECK_EQ(fclose(f), 0);
      fclose_timer.pauseTime();
      MPI_Barrier(MPI_COMM_WORLD);
    }
  }

  result.fopen_time = fopen_timer.getElapsedTime();
  result.fwrite_time = fwrite_timer.getElapsedTime();
  result.fclose_time = fclose_timer.getElapsedTime();
  result.compute_time = compute_timer.getElapsedTime();
  result.total_time = timer.getElapsedTime();

  return result;
}

void CheckResults(float *data, size_t num_elements,
                  const std::string &results_base, int rank) {
  std::string results_path = GetOutputPath(results_base, rank);

  FILE *f = fopen(results_path.c_str(), "r");
  CHECK(f);

  std::vector<float> read_data(num_elements);
  size_t num_bytes = num_elements * sizeof(float);
  size_t bytes_read = fread(read_data.data(), 1, num_bytes, f);
  Assert(bytes_read == num_bytes);

  for (size_t i = 0; i < num_elements; ++i) {
    Assert(data[i] == read_data[i]);
  }

  CHECK_EQ(fclose(f), 0);
}

int main(int argc, char* argv[]) {
  Options options = HandleArgs(argc, argv);

  MPI_Init(&argc, &argv);

  int rank = 0;
  int comm_size = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

  size_t num_elements = MEGABYTES(options.data_size_mb) / sizeof(float);
  const int kSectorSize = 512;
  float *data = (float *)aligned_alloc(kSectorSize,
                                       num_elements * sizeof(float));

  for (size_t i = 0; i < num_elements; ++i) {
    data[i] = uniform_random_number() * kXdim;
  }

  Timing local_results = {};

  if (options.do_posix_io) {
    hermes::testing::Timer timer;
    timer.resumeTime();
    local_results = RunPosixBench(options, data, rank);
    local_results.total_time = timer.pauseTime();
  } else {
    // RunHermesBench(options, data);
  }

  if (options.verify_results) {
    CheckResults(data, num_elements, options.output_path, rank);
  }

  Timing combined_results = {};
  MPI_Reduce(&local_results.total_time, &combined_results.total_time, 1,
             MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_results.fwrite_time, &combined_results.fwrite_time, 1,
             MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_results.fopen_time, &combined_results.fopen_time, 1,
             MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_results.fclose_time, &combined_results.fclose_time, 1,
             MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_results.compute_time, &combined_results.compute_time, 1,
             MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    double total_mb = options.data_size_mb * options.num_iterations * comm_size;
    double bandwidth_mbps = total_mb / combined_results.total_time;
    PrintResults(options, combined_results, bandwidth_mbps);
  }

  free(data);

  MPI_Finalize();

  return 0;
}
