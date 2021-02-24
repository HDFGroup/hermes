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

#include <cstdio>
#include <iostream>
#include <unordered_map>

#include <mpi.h>
#include "zlib.h"

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include "test_utils.h"

namespace hapi = hermes::api;
std::shared_ptr<hermes::api::Hermes> hermes_app;

int compress_blob(hermes::api::TraitInput &input, hermes::api::Trait *trait);
struct MyTrait : public hapi::Trait {
  int compress_level;
  MyTrait() : Trait(10001, hermes::TraitIdArray(), hermes::TraitType::META) {
    onAttachFn =
        std::bind(&compress_blob, std::placeholders::_1, std::placeholders::_2);
  }

  // optional function pointer if only known at runtime
};

void add_buffer_to_vector(hermes::api::Blob &vector, const char *buffer,
                          uLongf length) {
  for (uLongf character_index = 0; character_index < length;
       character_index++) {
    char current_character = buffer[character_index];
    vector.push_back(current_character);
  }
}

// The Trait implementer must define callbacks that match the VBucket::TraitFunc
// type.
int compress_blob(hermes::api::TraitInput &input, hermes::api::Trait *trait) {
  MyTrait *my_trait = (MyTrait *)trait;

  hapi::Context ctx;
  hapi::Bucket bkt(input.bucket_name, hermes_app, ctx);
  hapi::Blob blob = {};
  size_t blob_size = bkt.Get(input.blob_name, blob, ctx);
  blob.resize(blob_size);
  bkt.Get(input.blob_name, blob, ctx);
  bkt.Close(ctx);
  // If Hermes is already linked with a compression library, you can call the
  // function directly here. If not, the symbol will have to be dynamically
  // loaded and probably stored as a pointer in the Trait.
  uLongf source_length = blob.size();
  uLongf destination_length = compressBound(source_length);
  char *destination_data = new char[destination_length];

  LOG(INFO) << "Compressing blob\n";

  if (destination_data == nullptr)
    return Z_MEM_ERROR;

  int return_value = compress2((Bytef *) destination_data,
                               &destination_length, (Bytef *) blob.data(),
                               source_length, my_trait->compress_level);
  hermes::api::Blob destination {0};
  // TODO(KIMMY): where to store compressed data
  add_buffer_to_vector(destination, destination_data, destination_length);
  delete [] destination_data;

  return return_value;
}


void TestBucketPersist(std::shared_ptr<hapi::Hermes> hermes) {
  constexpr int bytes_per_blob = KILOBYTES(3);
  constexpr int num_blobs = 3;
  constexpr int total_bytes = num_blobs * bytes_per_blob;

  hapi::Context ctx;
  hapi::Blob blobx(bytes_per_blob, 'x');
  hapi::Blob bloby(bytes_per_blob, 'y');
  hapi::Blob blobz(bytes_per_blob, 'z');
  hapi::Bucket bkt("persistent_bucket", hermes, ctx);
  bkt.Put("blobx", blobx, ctx);
  bkt.Put("bloby", bloby, ctx);
  bkt.Put("blobz", blobz, ctx);

  std::string saved_file("blobsxyz.txt");
  bkt.Persist(saved_file, ctx);
  bkt.Destroy(ctx);
  Assert(!bkt.IsValid());

  FILE *bkt_file = fopen(saved_file.c_str(), "r");
  Assert(bkt_file);

  hermes::u8 read_buffer[total_bytes] = {};
  Assert(fread(read_buffer, 1, total_bytes, bkt_file) == total_bytes);

  for (int offset = 0; offset < num_blobs; ++offset) {
    for (int i = offset * bytes_per_blob;
         i < bytes_per_blob * (offset + 1);
         ++i) {
      char expected = '\0';
      switch (offset) {
        case 0: {
          expected = 'x';
          break;
        }
        case 1: {
          expected = 'y';
          break;
        }
        case 2: {
          expected = 'z';
          break;
        }
        default: {
          HERMES_INVALID_CODE_PATH;
        }
      }
      Assert(read_buffer[i] == expected);
    }
  }

  Assert(std::remove(saved_file.c_str()) == 0);
}

void TestPutOverwrite(std::shared_ptr<hapi::Hermes> hermes) {
  hapi::Context ctx;
  hapi::Bucket bucket("overwrite", hermes, ctx);

  std::string blob_name("1");
  size_t blob_size = KILOBYTES(6);
  hapi::Blob blob(blob_size, 'x');
  hermes::Status status = bucket.Put(blob_name, blob, ctx);
  Assert(status.Succeeded());

  hermes::testing::GetAndVerifyBlob(bucket, blob_name, blob);

  // NOTE(chogan): Overwrite the data
  size_t new_size = KILOBYTES(9);
  hapi::Blob new_blob(new_size, 'z');
  status = bucket.Put(blob_name, new_blob, ctx);

  hermes::testing::GetAndVerifyBlob(bucket, blob_name, new_blob);

  bucket.Destroy(ctx);
}

int main(int argc, char **argv) {
  try {
    int mpi_threads_provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
    if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
      fprintf(stderr, R"(Didn't receive appropriate MPI threading \
              specification\n)");
      return 1;
    }

    char *config_file = 0;
    if (argc == 2) {
      config_file = argv[1];
    }

    hermes_app = hermes::api::InitHermes(config_file);

    if (hermes_app->IsApplicationCore()) {
      hermes::api::Context ctx;

      hermes::api::Bucket my_bucket("compression", hermes_app, ctx);
      hermes_app->Display_bucket();
      hermes::api::Blob p1(1024*1024*400, 255);
      hermes::api::Blob p2(p1);
      my_bucket.Put("Blob1", p1, ctx);
      my_bucket.Put("Blob2", p2, ctx);

      if (my_bucket.ContainsBlob("Blob1"))
        std::cout<< "Found Blob1\n";
      else
        std::cout<< "Not found Blob1\n";
      if (my_bucket.ContainsBlob("Blob2"))
        std::cout<< "Found Blob2\n";
      else
        std::cout<< "Not found Blob2\n";

      hermes::api::VBucket my_vb("VB1", hermes_app, false, ctx);
      hermes_app->Display_vbucket();
      my_vb.Link("Blob1", "compression", ctx);
      my_vb.Link("Blob2", "compression", ctx);
      if (my_vb.Contain_blob("Blob1", "compression") == 1)
        std::cout << "Found Blob1 from compression bucket in VBucket VB1\n";
      else
        std::cout << "Not found Blob1 from compression bucket in VBucket VB1\n";
      if (my_vb.Contain_blob("Blob2", "compression") == 1)
        std::cout << "Found Blob2 from compression bucket in VBucket VB1\n";
      else
        std::cout << "Not found Blob2 from compression bucket in VBucket VB1\n";

      // compression level
      MyTrait trait;
      trait.compress_level = 6;
      my_vb.Attach(&trait, ctx);  // compress action to data starts

      TestBucketPersist(hermes_app);
      TestPutOverwrite(hermes_app);

    ///////
      my_vb.Unlink("Blob1", "VB1", ctx);
      my_vb.Detach(&trait, ctx);
    } else {
      // Hermes core. No user code here.
    }

    hermes_app->Finalize();

    MPI_Finalize();
  }
  catch (const std::runtime_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
  catch (const std::length_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
  catch ( ... ) {
    std::cout << "Catch exception\n";
  }

  return 0;
}
