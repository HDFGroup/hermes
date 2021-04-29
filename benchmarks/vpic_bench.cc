#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <numeric>

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
  bool shared_bucket;
  bool do_posix_io;
  bool direct_io;
};

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s [-bhiopx] \n", program);
  fprintf(stderr, "  -d (default false)\n");
  fprintf(stderr, "     Boolean flag. Do POSIX I/O with O_DIRECT.\n");
  fprintf(stderr, "  -h\n");
  fprintf(stderr, "     Print help\n");
  fprintf(stderr, "  -i <num_iterations> (default %d)\n", kDefaultIterations);
  fprintf(stderr, "     The number of times to run the VPIC I/O kernel.\n");
  fprintf(stderr, "  -n <num_nodes> (default 1)\n");
  fprintf(stderr, "     The number of nodes (only required when -x is used)n");
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

  int option = -1;
  while ((option = getopt(argc, argv, "dhi:n:o:p:sx")) != -1) {
    switch (option) {
      case 'd': {
        result.direct_io = true;
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

template<typename T>
static void DoFwrite(const std::vector<T> &vec, FILE *f) {
  size_t total_bytes = vec.size() * sizeof(T);
  size_t bytes_written = fwrite(vec.data(), 1, total_bytes, f);
  CHECK_EQ(bytes_written, total_bytes);
}

template<typename T>
static void DoWrite(const std::vector<T> &vec, int fd) {
  size_t total_bytes = vec.size() * sizeof(T);
  ssize_t bytes_written = write(fd, vec.data(), total_bytes);
  CHECK_EQ(bytes_written, total_bytes);
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

void RunHermesBench(const Options &options, const std::vector<float> &x) {
  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.num_devices = 1;
  config.num_targets = 1;
  config.capacities[0] = 128 * 1024 * 1024;
  // config.default_placement_policy = hapi::PlacementPolicy::kRandom;
  config.system_view_state_update_interval_ms = 500;
  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(&config);
  if (hermes->IsApplicationCore()) {
    int my_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    if (app_size != 1 && app_size != kNumVariables) {
      fprintf(stderr, "Must be run with 1 or 8 Application ranks"
              "(plus 1 Hermes rank)");
      exit(1);
    }

    MPI_Comm *comm = (MPI_Comm *)hermes->GetAppCommunicator();

    std::string bucket_name = "vpic_";

    if (options.shared_bucket) {
      bucket_name += "shared";
    } else {
      bucket_name += std::to_string(my_rank);
    }

    std::string output_file = bucket_name + ".out";

    hapi::Bucket bucket(bucket_name, hermes);

    hermes->AppBarrier();

    for (int i = 0; i < options.num_iterations; ++i) {
      auto start1 = now();
      if (app_size == 1) {
        for (int j = 0; j < kNumVariables; ++j) {
          std::string blob_name = std::to_string(j);
          CHECK(bucket.Put(blob_name, x).Succeeded());
        }
      } else {
        CHECK(bucket.Put("x", x).Succeeded());
      }
      auto end1 = now();

      double my_write_secs = duration<double>(end1 - start1).count();
      double avg_write_seconds = GetMPIAverage(my_write_secs, app_size, comm);

      auto start2 = now();
      CHECK(bucket.DeleteBlob("x").Succeeded());
      CHECK(bucket.DeleteBlob("y").Succeeded());
      CHECK(bucket.DeleteBlob("z").Succeeded());
      CHECK(bucket.DeleteBlob("px").Succeeded());
      CHECK(bucket.DeleteBlob("py").Succeeded());
      CHECK(bucket.DeleteBlob("pz").Succeeded());
      CHECK(bucket.DeleteBlob("id1").Succeeded());
      CHECK(bucket.DeleteBlob("id2").Succeeded());
      auto end2 = now();

      double my_flush_del_secs = duration<double>(end2 - start2).count();
      double avg_flush_del_seconds = GetMPIAverage(my_flush_del_secs, app_size,
                                                   comm);

      if (my_rank == 0) {
        printf("%d,%d,%d,%zu,%f,%f\n", options.do_posix_io,
               options.shared_bucket, options.num_iterations,
               options.data_size_mb, avg_write_seconds, avg_flush_del_seconds);
      }

      hermes->AppBarrier();
    }

    if (options.shared_bucket) {
      if (my_rank != 0) {
        bucket.Release();
      }
      hermes->AppBarrier();
      if (my_rank == 0) {
        bucket.Destroy();
      }
    } else {
      bucket.Destroy();
    }

  } else {
    // Hermes core. No user code.
  }

  hermes->Finalize();
}

double RunPosixBench(Options &options, const std::vector<float> &x, int rank) {
  hermes::testing::Timer timer;

  for (int i = 0; i < options.num_iterations; ++i) {
    std::string output_file = "vpic_posix_" + std::to_string(rank) + ".out";
    std::string output_path = (options.output_path + std::string("/") +
                               output_file);

    // int fd = open(output_path.c_str(),
    //               O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_SYNC,
    //               S_IRUSR | S_IWUSR);
    // CHECK(fd > 0);
    // timer.resumeTime();
    // DoWrite(x, fd);
    // timer.pauseTime();
    // CHECK_EQ(close(fd), 0);

    FILE *f = fopen(output_path.c_str(), "w");
    CHECK(f);

    timer.resumeTime();
    DoFwrite(x, f);
    timer.pauseTime();

    if (options.direct_io) {
      CHECK_EQ(fflush(f), 0);
      CHECK_EQ(fsync(fileno(f)), 0);
    }
    CHECK_EQ(fclose(f), 0);
  }

  double avg_total_seconds =
    GetMPIAverage(timer.getElapsedTime(), 8, MPI_COMM_WORLD);
  double total_mb =
    options.data_size_mb * kNumVariables * options.num_iterations;
  double bandwidth = total_mb / avg_total_seconds;

  return bandwidth;
}

int main(int argc, char* argv[]) {
  Options options = HandleArgs(argc, argv);

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  int rank = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0) {
    // TODO(chogan): DPE strategy, num Tiers, tier percentage
    // printf("posix,num_buckets,num_iterations,num_particles,write_seconds,"
    //        "flush_del_seconds\n");
  }

  size_t num_elements = MEGABYTES(options.data_size_mb) / sizeof(float);
  std::vector<float> data(num_elements);
  std::generate(data.begin(), data.end(), []() {
    return uniform_random_number() * kXdim;
  });

  // std::vector<float> x(num_elements);
  // std::vector<float> y(num_elements);
  // std::vector<float> z(num_elements);
  // std::vector<int> id1(num_elements);
  // std::vector<int> id2(num_elements);
  // std::vector<float> px(num_elements);
  // std::vector<float> py(num_elements);
  // std::vector<float> pz(num_elements);
  // InitParticles(kXdim, kYdim, kZdim, id1, id2, x, y, z, px, py, pz);

  if (options.do_posix_io) {
    double bandwidth = RunPosixBench(options, data, rank);
    if (rank == 0) {
      printf("%d,%d,%d,%d,%d,%zu,%f\n", options.do_posix_io, options.direct_io,
             options.num_nodes, options.shared_bucket, options.num_iterations,
             options.data_size_mb, bandwidth);
    }
  } else {
    RunHermesBench(options, data);
  }

  MPI_Finalize();

  return 0;
}
