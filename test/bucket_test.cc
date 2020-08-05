#include <iostream>
#include <unordered_map>

#include "zlib.h"

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include "common.h"

struct MyTrait {
  int compress_level;
  // optional function pointer if only known at runtime
};

void add_buffer_to_vector(hermes::api::Blob &vector, const char *buffer, uLongf length) {
  for (uLongf character_index = 0; character_index < length; character_index++) {
    char current_character = buffer[character_index];
    vector.push_back(current_character);
  }
}

// The Trait implementer must define callbacks that match the VBucket::TraitFunc type.
int compress_blob(hermes::api::Blob &blob, void *trait) {
  MyTrait *my_trait = (MyTrait *)trait;

  // If Hermes is already linked with a compression library, you can call the function directly here.
  // If not, the symbol will have to be dynamically loaded and probably stored as a pointer in the Trait.
  uLongf source_length = blob.size();
  uLongf destination_length = compressBound(source_length);
  char *destination_data = new char [destination_length];

  LOG(INFO) << "Compressing blob\n";

  if (destination_data == nullptr)
    return Z_MEM_ERROR;

  int return_value = compress2((Bytef *) destination_data,
                               &destination_length, (Bytef *) blob.data(), source_length,
                               my_trait->compress_level);
  hermes::api::Blob destination {0};
  //TODO: where to store compressed data
  add_buffer_to_vector(destination, destination_data, destination_length);
  delete [] destination_data;

  return return_value;
}

namespace hapi = hermes::api;

int main(int argc, char **argv)
{
  int mpi_threads_provided;
  MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }

  std::shared_ptr<hapi::Hermes> hermes_app = hermes::InitHermes(config_file);

  if (hermes_app->IsApplicationCore()) {
    hermes::api::Context ctx;

    hermes::api::Bucket my_bucket("compression", hermes_app, ctx);
    hermes_app->Display_bucket();
    hermes::api::Blob p1 (1024, 255);
    hermes::api::Blob p2 (p1);
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

    hermes::api::VBucket my_vb("VB1", hermes_app);
    hermes_app->Display_vbucket();
    my_vb.Link("Blob1", "compression", ctx);
    my_vb.Link("Blob2", "compression", ctx);
    if(my_vb.Contain_blob("Blob1", "compression") == 1)
      std::cout << "Found Blob1 from compression bucket in VBucket VB1\n";
    else
      std::cout << "Not found Blob1 from compression bucket in VBucket VB1\n";
    if(my_vb.Contain_blob("Blob2", "compression") == 1)
      std::cout << "Found Blob2 from compression bucket in VBucket VB1\n";
    else
      std::cout << "Not found Blob2 from compression bucket in VBucket VB1\n";

    // compression level
    struct MyTrait trait {6};
    my_vb.Attach(&trait, compress_blob, ctx);   // compress action to data starts

    ///////
    my_vb.Unlink("Blob1", "VB1", ctx);
    my_vb.Detach(&trait, ctx);
  } else {
    // Hermes core. No user code here.
  }

  hermes_app->Finalize();

  MPI_Finalize();

  return 0;
}
