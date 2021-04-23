#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <numeric>

#include "mpi.h"
#include "hermes.h"
#include "bucket.h"

namespace hapi = hermes::api;
using u8 = hermes::u8;

struct Options {
  int num_buckets;
  int num_iterations;
  size_t num_particles;
  bool do_posix_io;
  char *output_file;
};

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s Doc string goes here\n", program);
  fprintf(stderr, "  -b\n");
  fprintf(stderr, "    doc\n");
  fprintf(stderr, "  -i\n");
  fprintf(stderr, "    doc\n");
  fprintf(stderr, "  -o\n");
  fprintf(stderr, "    doc\n");
  fprintf(stderr, "  -p\n");
  fprintf(stderr, "    doc\n");
  fprintf(stderr, "  -x\n");
  fprintf(stderr, "    doc\n");
}

Options HandleArgs(int argc, char **argv) {
  Options result = {};
  result.num_buckets = 1;
  result.num_iterations = 16;
  result.num_particles = (8 * 1024 * 1024) / sizeof(float);
  result.output_path = "./";

  int option = -1;
  while ((option = getopt(argc, argv, "b:i:o:p:h")) != -1) {
    switch (option) {
      case 'h': {
        PrintUsage(argv[0]);
        exit(0);
      }
      case 'b': {
        result.num_buckets = atoi(optarg);
        break;
      }
      case 'i': {
        result.num_iterations = atoi(optarg);
        break;
      }
      case 'o': {
        result.output_file = optarg;
        break;
      }
      case 'p': {
        result.num_particles = (size_t)std::stoull(optarg);
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

int main(int argc, char* argv[]) {
  const int kNumVariables = 8;
  const int kXdim = 64;
  const int kYdim = 64;
  const int kZdim = 64;
  const auto now = std::chrono::high_resolution_clock::now;

  Options options = HandleArgs(argc, argv);

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.num_devices = 1;
  config.num_targets = 1;
  config.capacities[0] = 256 * 1024 * 1024;
  config.default_placement_policy = hapi::PlacementPolicy::kRandom;
  config.system_view_state_update_interval_ms = 1;
  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(&config);

  if (hermes->IsApplicationCore()) {
    int my_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    if (options.num_buckets != 1 || options.num_buckets != kNumVariables ||
        options.num_buckets != app_size) {

    }

    std::vector<int> id1(options.num_particles);
    std::vector<int> id2(options.num_particles);
    std::vector<float> x(options.num_particles);
    std::vector<float> y(options.num_particles);
    std::vector<float> z(options.num_particles);
    std::vector<float> px(options.num_particles);
    std::vector<float> py(options.num_particles);
    std::vector<float> pz(options.num_particles);
    InitParticles(kXdim, kYdim, kZdim, id1, id2, x, y, z, px, py, pz);

    std::string bucket_name = "vpic_" + std::to_string(my_rank);
    std::string output_file = bucket_name + ".out";

    std::vector<hapi::Bucket> buckets;
    for (int i = 0; i < options.num_buckets; ++i) {
      buckets.emplace_back(bucket_name, hermes);
    }
    hermes->AppBarrier();

    auto start = now();

    for (int i = 0; i < options.num_iterations; ++i) {
      FILE *f = NULL;

      auto start1 = now();
      if (options.do_posix_io) {
        f = fopen(output_file.c_str(), "w");
        CHECK(f);
        DoFwrite(x, f);
        DoFwrite(y, f);
        DoFwrite(z, f);
        DoFwrite(id1, f);
        DoFwrite(id2, f);
        DoFwrite(px, f);
        DoFwrite(py, f);
        DoFwrite(pz, f);
      } else {
        CHECK(bucket.Put("x", x).Succeeded());
        CHECK(bucket.Put("y", y).Succeeded());
        CHECK(bucket.Put("z", z).Succeeded());
        CHECK(bucket.Put("id1", id1).Succeeded());
        CHECK(bucket.Put("id2", id2).Succeeded());
        CHECK(bucket.Put("px", px).Succeeded());
        CHECK(bucket.Put("py", py).Succeeded());
        CHECK(bucket.Put("pz", pz).Succeeded());
      }
      auto end1 = now();

      auto start2 = now();
      if (options.do_posix_io) {
        CHECK_EQ(fflush(f), 0);
        CHECK_EQ(fsync(fileno(f)), 0);
        CHECK_EQ(fclose(f), 0);
      } else {
        CHECK(bucket.DeleteBlob("x").Succeeded());
        CHECK(bucket.DeleteBlob("y").Succeeded());
        CHECK(bucket.DeleteBlob("z").Succeeded());
        CHECK(bucket.DeleteBlob("px").Succeeded());
        CHECK(bucket.DeleteBlob("py").Succeeded());
        CHECK(bucket.DeleteBlob("pz").Succeeded());
        CHECK(bucket.DeleteBlob("id1").Succeeded());
        CHECK(bucket.DeleteBlob("id2").Succeeded());
      }
      auto end2 = now();

      hermes->AppBarrier();

    }

    auto end = now();

    if (my_rank == 0) {
      printf("num_buckets,num_iterations,num_particles\n");
      double total_seconds_writing =
        std::chrono::duration<double>(end1 - start1).count();
      printf("Seconds spent writing: %f\n", total_seconds_writing);
      double total_seconds_flushing =
        std::chrono::duration<double>(end2 - start2).count();
      printf("Seconds spent flushing: %f\n", total_seconds_flushing);
      double total_seconds = std::chrono::duration<double>(end - start).count();
      printf("Total seconds: %f\n", total_seconds);
    }
  } else {
    // Hermes core. No user code.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
