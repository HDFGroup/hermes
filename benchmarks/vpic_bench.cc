#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <algorithm>
#include <chrono>
#include <numeric>

#include "mpi.h"
#include "hermes.h"
#include "bucket.h"

namespace hapi = hermes::api;
using u8 = hermes::u8;

static inline double uniform_random_number() {
  return (((double)rand())/((double)(RAND_MAX)));
}

static void MasterPrintf(int rank, const char *msg) {
  if (rank == 0) {
    printf("%s", msg);
  }
}

template<typename T>
void DoFwrite(const std::vector<T> &vec, FILE *f) {
  size_t total_bytes = vec.size() * sizeof(T);
  size_t bytes_written = fwrite(vec.data(), 1, total_bytes, f);
  CHECK_EQ(bytes_written, total_bytes);
}

int main(int argc, char* argv[]) {
  const auto now = std::chrono::high_resolution_clock::now;
  bool use_hermes = true;

  size_t num_particles = (8 * 1024 * 1024) / sizeof(float);
  if (argc == 2) {
    num_particles = (std::stoul(argv[2])) * 1024UL * 1024UL;
  }

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

    if (my_rank == 0) {
      printf("Number of particles: %zu \n", num_particles);
    }

    const int kXdim = 64;
    const int kYdim = 64;
    const int kZdim = 64;

    // Init particles
    std::vector<int> id1(num_particles);
    std::iota(id1.begin(), id1.end(), 0);

    std::vector<int> id2(num_particles);
    for (size_t i = 0; i < num_particles; ++i) {
      id2[i] = id1[i] * 2;
    }

    std::vector<float> x(num_particles);
    std::generate(x.begin(), x.end(), [&kXdim]() {
      return uniform_random_number() * kXdim;
    });

    std::vector<float> y(num_particles);
    std::generate(y.begin(), y.end(), [&kYdim]() {
      return uniform_random_number() * kYdim;
    });

    std::vector<float> z(num_particles);
    for (size_t i = 0; i < num_particles; ++i) {
      z[i] = ((double)id1[i] / num_particles) * kZdim;
    }

    std::vector<float> px(num_particles);
    std::generate(px.begin(), px.end(), [&kXdim]() {
      return uniform_random_number() * kXdim;
    });

    std::vector<float> py(num_particles);
    std::generate(py.begin(), py.end(), [&kYdim]() {
      return uniform_random_number() * kYdim;
    });

    std::vector<float> pz(num_particles);
    for (size_t i = 0; i < num_particles; ++i) {
      pz[i] = ((double)id2[i] / num_particles) * kZdim;
    }

    hermes->AppBarrier();
    // MasterPrintf(my_rank, "Finished initializing particles \n");

    auto start = now();

    std::string bucket_name = "vpic_" + std::to_string(my_rank);
    std::string output_file = bucket_name + ".out";
    hapi::Bucket bucket(bucket_name, hermes);

    hermes->AppBarrier();
    // MasterPrintf(my_rank, "Before writing particles \n");

    auto start1 = now();

    FILE *f = NULL;

    if (use_hermes) {
      CHECK(bucket.Put("x", x).Succeeded());
      CHECK(bucket.Put("y", y).Succeeded());
      CHECK(bucket.Put("z", z).Succeeded());
      CHECK(bucket.Put("id1", id1).Succeeded());
      CHECK(bucket.Put("id2", id2).Succeeded());
      CHECK(bucket.Put("px", px).Succeeded());
      CHECK(bucket.Put("py", py).Succeeded());
      CHECK(bucket.Put("pz", pz).Succeeded());
    } else {
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
    }

    hermes->AppBarrier();
    auto end1 = now();
    // MasterPrintf(my_rank, "After writing particles \n");

    auto start2 = now();
    if (use_hermes) {
      // bucket.Persist(bucket_name + ".out");
    } else {
      CHECK_EQ(fflush(f), 0);
      CHECK_EQ(fsync(fileno(f)), 0);
      CHECK_EQ(fclose(f), 0);
    }
    auto end2 = now();

    CHECK(bucket.Destroy().Succeeded());

    hermes->AppBarrier();

    auto end = now();

    if (my_rank == 0) {
      printf("Timing results:\n");
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
