//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "posix_bdev/posix_bdev.h"
#include "hermes/slab_allocator.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

namespace hermes::posix_bdev {

class Server : public TaskLib {
 public:
  SlabAllocator alloc_;
  char *mem_ptr_;
  int fd_;
  std::string path_;

 public:
  void Construct(ConstructTask *task) {
    DeviceInfo &dev_info = task->info_;
    alloc_.Init(id_, dev_info.capacity_, dev_info.slab_sizes_);
    std::string text = dev_info.mount_dir_ +
        "/" + "slab_" + dev_info.dev_name_;
    auto canon = stdfs::weakly_canonical(text).string();
    dev_info.mount_point_ = canon;
    path_ = canon;
    fd_ = open(dev_info.mount_point_.c_str(),
                  O_TRUNC | O_CREAT, 0666);
    if (fd_ < 0) {
      HELOG(kError, "Failed to open file: {}", dev_info.mount_point_);
    }
    HILOG(kInfo, "Created {} at {} of size {}",
          dev_info.dev_name_, dev_info.mount_point_, dev_info.capacity_);
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task) {
    free(mem_ptr_);
    task->SetModuleComplete();
  }

  void Alloc(AllocateTask *task) {
    alloc_.Allocate(task->size_, *task->buffers_, task->alloc_size_);
    HILOG(kDebug, "Allocated {}/{} bytes ({})", task->alloc_size_, task->size_, path_);
    task->SetModuleComplete();
  }

  void Free(FreeTask *task) {
    alloc_.Free(task->buffers_);
    task->SetModuleComplete();
  }

  void Write(WriteTask *task) {
    size_t count = pwrite(fd_, task->buf_, task->size_, (off_t)task->disk_off_);
    if (count != task->size_) {
      HELOG(kError, "BORG: wrote {} bytes, but expected {}",
            count, task->size_);
    }
    task->SetModuleComplete();
  }

  void Read(ReadTask *task) {
    memcpy(task->buf_, mem_ptr_ + task->disk_off_, task->size_);
    size_t count = pread(fd_, task->buf_, task->size_, (off_t)task->disk_off_);
    if (count != task->size_) {
      HELOG(kError, "BORG: read {} bytes, but expected {}",
            count, task->size_);
    }
    task->SetModuleComplete();
  }

  void Monitor(MonitorTask *task) {
  }

  void UpdateCapacity(UpdateCapacityTask *task) {
    task->SetModuleComplete();
  }

 public:
#include "bdev/bdev_lib_exec.h"
};

}  // namespace labstor

LABSTOR_TASK_CC(hermes::posix_bdev::Server, "posix_bdev");
