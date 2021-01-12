#include <bucket.h>
#include <hermes.h>
#include <vbucket.h>

#include <experimental/filesystem>

#include "catch_config.h"

namespace fs = std::experimental::filesystem;

struct Arguments {
  std::string filename = "test.dat";
  std::string directory = "/tmp";
  std::string config = "./hermes.conf";
  size_t request_size = 16 * 1024;
  size_t iterations = 1024;
};

struct Info {
  int rank;
  int comm_size;
  hermes::api::Blob write_blob;
  const size_t FILE_PAGE = 16 * 1024;
};

Arguments args;
Info info;

cl::Parser define_options() {
  return cl::Opt(args.filename, "filename")["-f"]["--filename"](
             "Filename used for performing I/O") |
         cl::Opt(args.directory, "dir")["-d"]["--directory"](
             "Directory used for performing I/O") |
         cl::Opt(args.config,
                 "config")["-c"]["--config"]("Configuration file for hermes") |
         cl::Opt(args.request_size, "request_size")["-s"]["--request_size"](
             "Request size for each operation") |
         cl::Opt(args.iterations,
                 "iterations")["-z"]["--iterations"]("Number of iterations");
}

int init() {
  MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
  MPI_Comm_size(MPI_COMM_WORLD, &info.comm_size);
  return 0;
}

int finalize() { return 0; }

int pretest() { info.write_blob = hermes::api::Blob(args.request_size, '1'); }
int posttest() {}

int flush_blob(hermes::api::TraitInput &input, void *trait) {}

TEST_CASE("CustomTrait",
          "[module=trait][type=meta][trait=FileBacked]"
          "[process=" +
              std::to_string(info.comm_size) + "]") {
  pretest();
  // REQUIRE(info.comm_size > 1);
  std::shared_ptr<hermes::api::Hermes> hermes_app =
      hermes::api::InitHermes(args.config.c_str(), true);
  hermes::api::Context ctx;
  fs::path fullpath = args.directory;
  fullpath /= args.filename;
  std::string fullpath_str = fullpath.string();
  SECTION("Basic") {
    auto flush_blob = [](hermes::api::TraitInput &input,
                         hermes::api::Trait *trait) {
      hermes::api::FileBackedTrait *t =
          static_cast<hermes::api::FileBackedTrait *>(trait);
      FILE *fh = fopen(t->filename.c_str(), "r+");
      REQUIRE(fh != nullptr);
      size_t offset = std::stoi(input.blob_name) * info.FILE_PAGE;
      if (offset > 0) {
        int status = fseek(fh, offset, SEEK_SET);
        REQUIRE(status == 0);
      }
      auto items =
          fwrite(input.blob.data(), input.blob.size(), sizeof(char), fh);
      REQUIRE(items == sizeof(char));
      int status = fclose(fh);
      REQUIRE(status == 0);
      return 0;
    };
    auto load_blob = [](hermes::api::TraitInput &input,
                        hermes::api::Trait *trait) {
      hermes::api::FileBackedTrait *t =
          static_cast<hermes::api::FileBackedTrait *>(trait);
      size_t required_file_size =
          std::stoi(input.blob_name) * info.FILE_PAGE + info.FILE_PAGE;
      size_t current_file_size = 0;
      if (fs::exists(t->filename)) {
        current_file_size = fs::file_size(t->filename);
      }
      if (current_file_size < required_file_size) {
        std::ofstream ofs(t->filename, std::ios::binary | std::ios::out);
        ofs.seekp(required_file_size - 1);
        ofs.write("", 1);
      }
      current_file_size = fs::file_size(t->filename);
      REQUIRE(current_file_size >= required_file_size);
      return 0;
    };
    hermes::api::Bucket file_bucket(args.filename, hermes_app, ctx);
    hermes::api::VBucket file_vbucket(args.filename, hermes_app, ctx);
    auto offset_map = std::unordered_map<std::string, hermes::u64>();
    auto trait = hermes::api::FileBackedTrait(fullpath_str, offset_map, true,
                                              flush_blob, true, load_blob);
    file_vbucket.Attach(&trait, ctx);
    for (int i = 0; i < args.iterations; ++i) {
      file_bucket.Put(std::to_string(i), info.write_blob, ctx);
      file_vbucket.Link(std::to_string(i), args.filename, ctx);
    }
    file_vbucket.Detach(&trait, ctx);
    file_vbucket.Delete(ctx);
    file_bucket.Destroy(ctx);
  }
  hermes_app->Finalize(true);
  posttest();
}
