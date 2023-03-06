//
// Created by lukemartinlogan on 2/22/23.
//

#include <hermes_shm/data_structures/internal/shm_container.h>
#include <hermes_shm/data_structures/internal/shm_archive.h>
#include <hermes_shm/thread/lock.h>
#include <hermes_shm/data_structures/thread_unsafe/vector.h>

template<typename T>
class ShmHeader;

class LockedVector;

template<>
class ShmHeader<LockedVector> : public hipc::ShmBaseHeader {
 public:
  hermes_shm::Mutex lock_;
  hipc::ShmArchive<hipc::vector<int>> vec_;

  ShmHeader(hipc::Allocator *alloc) {
    vec_.shm_init(alloc);
  }
};

class LockedVector {
 public:
 SHM_CONTAINER_TEMPLATE((LockedVector), (LockedVector), (ShmHeader<LockedVector>))

 private:
  /**====================================
   * Variables
   * ===================================*/
  hipc::ShmRef<hipc::vector<int>> vec_;

 public:
  /**====================================
   * Shm Overrides
   * ===================================*/

  /** Default constructor */
  LockedVector() = default;

  /** Default shm constructor. Initializes the vector with 0 elements. */
  void shm_init_main(header_t *header,
                     hipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header, alloc_);
    shm_deserialize_main();
  }

  /** Move constructor */
  void shm_weak_move_main(ShmHeader<LockedVector> *header,
                          hipc::Allocator *alloc,
                          LockedVector &other) {
    shm_init_main(header, alloc);
    (*vec_) = std::move(*other.vec_);
  }

  /** Copy constructor */
  void shm_strong_copy_main(ShmHeader<LockedVector> *header,
                            hipc::Allocator *alloc,
                            const LockedVector &other) {
    shm_init_main(header, alloc);
    (*vec_) = (*other.vec_);
  }

  /** Destroy the shared-memory data. */
  void shm_destroy_main() {
    vec_->shm_destroy();
  }

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {
    vec_->shm_deserialize(header_->vec_.internal_ref(alloc_));
  }

  /**====================================
   * Custom Additions
   * ===================================*/
  void emplace_back(int num) {
    vec_->emplace_back(num);
  }

  hipc::ShmRef<int> operator[](size_t i) {
    return (*vec_)[i];
  }
};