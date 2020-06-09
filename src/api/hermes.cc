#include "sys/mman.h"

#include "glog/logging.h"

#include "hermes.h"
#include "bucket.h"

namespace hermes {

namespace api {

Status RenameBucket(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx) {
  (void)ctx;
  Status ret = 0;

  LOG(INFO) << "Renaming Bucket from " << old_name << " to " << new_name << '\n';

  return ret;
}

Status TransferBlob(const Bucket &src_bkt,
                    const std::string &src_blob_name,
                    Bucket &dst_bkt,
                    const std::string &dst_blob_name,
                    Context &ctx) {
  (void)src_bkt;
  (void)dst_bkt;
  (void)ctx;
  Status ret = 0;

  LOG(INFO) << "Transferring Blob from " << src_blob_name << " to "
            << dst_blob_name << '\n';

  return ret;
}

bool Hermes::IsApplicationCore() {
  bool result = comm_.proc_kind == ProcessKind::kApp;

  return result;
}

void Hermes::AppBarrier() {
  hermes::SubBarrier(&comm_);
}

int Hermes::GetProcessRank() {
  int result = comm_.sub_proc_id;

  return result;
}

int Hermes::GetNumProcesses() {
  int result = comm_.app_size;

  return result;
}

void Hermes::Finalize() {
  hermes::Finalize(&context_, &comm_, shmem_name_.c_str(), &trans_arena_,
                   IsApplicationCore());
}

} // api namepsace
} // hermes namespace
