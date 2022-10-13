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

#include "../../stagers/unix_stager.h"
#include <cstdlib>

using hermes::UnixStager;
using hermes::PlacementPolicy;
using hermes::adapter::posix::PosixFS;
namespace stdfs = std::experimental::filesystem;

void CreateFile(const std::string &path, int nonce, size_t size) {
  size_t count = size / sizeof(int);
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  int rank = 0;
  if (mdm->is_mpi) {
    rank = mdm->rank;
  }
  if (rank == 0) {
    FILE *file = fopen(path.c_str(), "w");
    std::vector<int> data(count, nonce);
    fwrite(data.data(), 1, size, file);
    fclose(file);
  }
  if (mdm->is_mpi) {
    MPI_Barrier(MPI_COMM_WORLD);
  }
}

void VerifyFile(const std::string &path, int nonce, size_t size) {
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  if (mdm->rank != 0) {
    return;
  }
  size_t count = size / sizeof(int);

  if (stdfs::file_size(path) != size) {
    std::cout << "File sizes are not equal" << std::endl;
    exit(1);
  }

  FILE *file = fopen(path.c_str(), "r");
  std::vector<int> data(count);
  fread(data.data(), 1, size, file);
  for (size_t i = 0; i < count ; ++i) {
    if (data[i] != nonce) {
      std::cout << "File verification failed"
                << " at offset: " << i << "/" << count << std::endl;
      exit(1);
    }
  }
  fclose(file);
}

void test_file_stage() {
  UnixStager stager;
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  std::string path = "/tmp/test.txt";
  int nonce = mdm->rank + 122;
  int new_nonce = ~nonce;
  size_t file_size = MEGABYTES(16);
  PlacementPolicy dpe = PlacementPolicy::kRoundRobin;

  // Create a 16MB file
  CreateFile(path, nonce, file_size);
  VerifyFile(path, nonce, file_size);

  // Stage in the 16MB file
  stager.StageIn(path, PlacementPolicy::kRoundRobin);
  if (mdm->is_mpi) { MPI_Barrier(MPI_COMM_WORLD); }

  // Ensure the bucket corresponding to the file name is created
  bool bucket_exists = mdm->GetHermes()->BucketExists(path);
  if (!bucket_exists) {
    std::cout << "Stage in failed to load bucket" << std::endl;
    exit(1);
  }
  auto bkt = std::make_shared<hapi::Bucket>(path, mdm->GetHermes());

  // Verify the bucket contents
  if (mdm->rank == 0) {
    for (int i = 0; i < 16; ++i) {
      // A blob should be created for each 1MB of data
      // This is because the hermes file page size (kPageSize) is 1MB
      hermes::adapter::BlobPlacement p;
      p.page_ = i;
      std::string blob_name = p.CreateBlobName();
      bool has_blob = bkt->ContainsBlob(blob_name);
      if (!has_blob) {
        std::cout << "Stage in failed to load blob: " << i
                  << " in file: " << path << std::endl;
        exit(1);
      }

      // Get blob size and contents
      hapi::Blob blob(0);
      auto size = bkt->Get(blob_name, blob);
      blob.resize(size);
      bkt->Get(blob_name, blob);
      auto count = size / sizeof(int);
      int *blob_data = (int *)blob.data();

      // Ensure the blob contents are correct
      for (size_t off = 0; off < count; ++off) {
        if (blob_data[off] != nonce) {
          std::cout << "Stage-in has improper content " << blob_data[off]
                    << " != " << nonce << " in file: " << path << std::endl;
          exit(1);
        }
      }
    }

    // Override the original blob contents with new contents
    auto count = file_size / sizeof(int);
    std::vector<int> new_data(count, new_nonce);
    bool stat_exists;
    AdapterStat stat;
    PosixFS fs_api;
    IoStatus io_status;
    File f = fs_api.Open(stat, path);
    fs_api.Write(f, stat, new_data.data(), 0, file_size, io_status,
                 IoOptions::WithParallelDpe(dpe));
    fs_api.Close(f, stat_exists, false);
  }

  // Stage out the new file (sync its contents)
  MPI_Barrier(MPI_COMM_WORLD);
  stager.StageOut(path);

  // Verify the contents have been written
  VerifyFile(path, new_nonce, file_size);
}

void test_directory_stage() {

}

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  mdm->InitializeHermes(true);
  test_file_stage();
  mdm->FinalizeHermes();
  MPI_Finalize();
  std::cout << "Evaluation passed!" << std::endl;
}