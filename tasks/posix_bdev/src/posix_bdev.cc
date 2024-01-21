//
// Created by lukemartinlogan on 6/29/23.
//

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "posix_bdev/posix_bdev.h"
#include "hermes/slab_allocator.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
// #include <libaio.h>

namespace hermes::posix_bdev {

class Server : public TaskLib, public bdev::Server {
 public:
  SlabAllocator alloc_;
  int fd_;
  std::string path_;

 public:
  /** Construct posix BDEV */
  void Construct(ConstructTask *task, RunContext &rctx) {
    DeviceInfo &dev_info = task->info_;
    rem_cap_ = dev_info.capacity_;
    alloc_.Init(id_, dev_info.capacity_, dev_info.slab_sizes_);
    score_hist_.Resize(10);
    std::string text = dev_info.mount_dir_ +
        "/" + "slab_" + dev_info.dev_name_;
    auto canon = stdfs::weakly_canonical(text).string();
    dev_info.mount_point_ = canon;
    path_ = canon;
    fd_ = open(dev_info.mount_point_.c_str(),
               O_TRUNC | O_CREAT | O_RDWR, 0666);
    if (fd_ < 0) {
      HELOG(kError, "Failed to open file: {}", dev_info.mount_point_);
    }
    HILOG(kInfo, "Created {} at {} of size {}",
          dev_info.dev_name_, dev_info.mount_point_, dev_info.capacity_);
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy posix bdev */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** Allocate space from bdev */
  void Allocate(AllocateTask *task, RunContext &rctx) {
    alloc_.Allocate(task->size_, *task->buffers_, task->alloc_size_);
    HILOG(kDebug, "Allocated {}/{} bytes ({})", task->alloc_size_, task->size_, path_);
    rem_cap_ -= task->alloc_size_;
    score_hist_.Increment(task->score_);
    task->SetModuleComplete();
  }
  void MonitorAllocate(u32 mode, AllocateTask *task, RunContext &rctx) {
  }

  /** Free space from bdev */
  void Free(FreeTask *task, RunContext &rctx) {
    rem_cap_ += alloc_.Free(task->buffers_);
    score_hist_.Decrement(task->score_);
    task->SetModuleComplete();
  }
  void MonitorFree(u32 mode, FreeTask *task, RunContext &rctx) {
  }

  /** Write to bdev */
  void Write(WriteTask *task, RunContext &rctx) {
    HILOG(kDebug, "Writing {} bytes to {}", task->size_, path_);
#ifdef HERMES_LIBAIO
    switch (task->phase_) {
      case 0: {
        int ret = io_setup(1, &task->ctx_);
        if (ret < 0) {
          perror("io_setup");
          HELOG(kError, "Libaio failed for write (1)");
          task->SetModuleComplete();
          return;
        }
        struct iocb xfer_iocb;
        struct iocb *iocb_list[1];
        io_prep_pwrite(&xfer_iocb, fd_, (void*)task->buf_, task->size_, task->disk_off_);
        xfer_iocb.data = (void*) task->buf_;
        iocb_list[0] = &xfer_iocb;
        ret = io_submit(task->ctx_, 1, iocb_list);
        if (ret != 1) {
          perror("io_submit");
          HELOG(kError, "Libaio failed for write (2)");
          task->SetModuleComplete();
          return;
        }
        task->phase_ = 1;
      }
      case 1: {
        struct io_event events[1];
        struct timespec timeout{0, 0};
        int ret = io_getevents(task->ctx_, 1, 1, events, &timeout);
        if (ret == -EAGAIN) {
          return;
        } else if (ret < 0) {
          perror("io_getevents");
          HELOG(kError, "Libaio failed for write (3)");
          task->SetModuleComplete();
          return;
        }
        io_destroy(task->ctx_);
      }
    }
#else
    ssize_t count = pwrite64(fd_, task->buf_, task->size_, (off64_t)task->disk_off_);
    if (count != task->size_) {
      HELOG(kError, "BORG: wrote {} bytes, but expected {}: {}",
            count, task->size_, strerror(errno));
    }
#endif
    task->SetModuleComplete();
  }
  void MonitorWrite(u32 mode, WriteTask *task, RunContext &rctx) {
  }

  /** Read from bdev */
  void Read(ReadTask *task, RunContext &rctx) {
    HILOG(kDebug, "Reading {} bytes from {}", task->size_, path_);
#ifdef HERMES_LIBAIO
    switch (task->phase_) {
      case 0: {
        int ret = io_setup(1, &task->ctx_);
        if (ret < 0) {
          perror("io_setup");
          HELOG(kError, "Libaio failed for write (1)");
          task->SetModuleComplete();
          return;
        }
        struct iocb xfer_iocb;
        struct iocb *iocb_list[1];
        io_prep_pread(&xfer_iocb, fd_, (void*)task->buf_, task->size_, task->disk_off_);
        xfer_iocb.data = (void*) task->buf_;
        iocb_list[0] = &xfer_iocb;
        ret = io_submit(task->ctx_, 1, iocb_list);
        if (ret != 1) {
          perror("io_submit");
          HELOG(kError, "Libaio failed for write (2)");
          task->SetModuleComplete();
          return;
        }
        task->phase_ = 1;
      }
      case 1: {
        struct io_event events[1];
        struct timespec timeout{0, 0};
        int ret = io_getevents(task->ctx_, 1, 1, events, &timeout);
        if (ret == -EAGAIN) {
          return;
        } else if (ret < 0) {
          perror("io_getevents");
          HELOG(kError, "Libaio failed for write (3)");
          task->SetModuleComplete();
          return;
        }
        io_destroy(task->ctx_);
      }
    }
#else
    ssize_t count = pread64(fd_, task->buf_, task->size_, (off64_t)task->disk_off_);
    if (count != task->size_) {
      HELOG(kError, "BORG: read {} bytes, but expected {}",
            count, task->size_);
    }
#endif
    task->SetModuleComplete();
  }
  void MonitorRead(u32 mode, ReadTask *task, RunContext &rctx) {
  }
 public:
#include "bdev/bdev_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hermes::posix_bdev::Server, "posix_bdev");
