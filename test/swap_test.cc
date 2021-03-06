#include <bucket.h>
#include <hermes.h>
#include <mpi.h>

namespace hapi = hermes::api;
std::shared_ptr<hermes::api::Hermes> hermes_app;

int main(int argc, char **argv) {
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

  hermes_app = hermes::api::InitHermes(config_file);

  if (hermes_app->IsApplicationCore()) {
    hermes::api::Context ctx;

    hermes::api::Bucket my_bucket("swap", hermes_app, ctx);
    hermes_app->Display_bucket();
    size_t blob_size = 1024*1024;
    hermes::api::Blob p1(blob_size, 255);
    for(int i=0;i<256;++i){
      std::string blob_name = "Blob" + std::to_string(i);
      my_bucket.Put(blob_name, p1, ctx);
    }
    for(int i=0;i<256;++i){
      std::string blob_name = "Blob" + std::to_string(i);
      assert(my_bucket.ContainsBlob(blob_name));
    }
    for(int i=0;i<256;++i){
      std::string blob_name = "Blob" + std::to_string(i);
      hermes::api::Blob read_blob(blob_size, 254);
      my_bucket.Get(blob_name, read_blob,ctx);
      assert(read_blob == p1);
    }
    my_bucket.Destroy(ctx);
  } else {
    // Hermes core. No user code here.
  }

  hermes_app->Finalize();

  MPI_Finalize();

  return 0;
}
