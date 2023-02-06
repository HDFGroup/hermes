//
// Created by lukemartinlogan on 1/26/23.
//

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_STRUCT_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_STRUCT_H_

#include "hermes_shm/data_structures/internal/shm_container.h"
#include "hermes_shm/data_structures/internal/shm_archive_or_t.h"
#include "hermes_shm/data_structures/internal/shm_null_container.h"
#include "hermes_shm/types/tuple_base.h"

#define CLASS_NAME ShmStruct
#define TYPED_CLASS ShmStruct<Containers...>
#define TYPED_HEADER ShmHeader<ShmStruct<Containers...>>

namespace hermes_shm::ipc {

/** Tuple forward declaration */
template<typename ...Containers>
class ShmStruct;

/** Tuple SHM header */
template<typename ...Containers>
struct ShmHeader<ShmStruct<Containers...>> {
  /**< All object headers */
  hermes_shm::tuple_wrap<ShmHeaderOrT, Containers...> hdrs_;

  /** Default initialize headers */
  ShmHeader() = default;

  /** Piecewise initialize all headers */
  template<typename ...Args>
  explicit ShmHeader(Allocator *alloc, Args&& ...args) {
    ForwardIterateTuple::Apply(
      hdrs_,
      [alloc, args=make_argpack(std::forward<Args>(args)...)]
      (auto i, auto &obj_hdr) constexpr {
        obj_hdr.PiecewiseInit(alloc, args.template Forward<i.Get()>());
      });
  }

  /** Get the internal reference to the ith object */
  template<size_t i>
  decltype(auto) internal_ref(Allocator *alloc) {
    return hdrs_.template Get<i>().internal_ref(alloc);
  }

  /** Destructor */
  void shm_destroy(Allocator *alloc) {
    hermes_shm::ForwardIterateTuple::Apply(
      hdrs_,
      [alloc](size_t i, auto &obj_hdr) constexpr {
        obj_hdr.shm_destroy(alloc);
      }
    );
  }
};

/** A tuple of objects to store in shared memory */
template<typename ...Containers>
class ShmStruct : public ShmContainer {
 public:
  /**====================================
   * Variables & Types
   *===================================*/

  typedef TYPED_HEADER header_t; /**< Index to header type */
  header_t *header_; /**< The shared-memory header */
  Allocator *alloc_; /**< Allocator used for the header */
  hermes_shm::tuple<Containers...>
    objs_; /**< Constructed objects */

 public:
  /**====================================
   * Shm Overrides
   * ===================================*/

  /** Default constructor */
  CLASS_NAME() = delete;

  /** Default shm constructor */
  template<typename ...Args>
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc) {
    shm_init_header(header, alloc);
  }

  /** Piecewise shm constructor */
  template<typename ...Args>
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc,
                     Args&& ...args) {
    shm_init_header(header, alloc,
                    std::forward<Args>(args)...);
  }

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc, CLASS_NAME &other) {
    shm_init_header(header, alloc, std::move(other));
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc, const CLASS_NAME &other) {
    shm_init_header(header, alloc, other);
  }

  /** Destroy the shared-memory data. */
  void shm_destroy_main() {}

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /**====================================
   * Constructors
   * ===================================*/

  /** Constructor. Allocate header with default allocator. */
  template<typename ...Args>
  explicit CLASS_NAME(Args&& ...args) {
    shm_init(std::forward<Args>(args)...);
  }

  /** Constructor. Allocate header with default allocator. */
  template<typename ...Args>
  void shm_init(Args&& ...args) {
    shm_destroy(false);
    shm_init_main(typed_nullptr<TYPED_HEADER>(),
                  typed_nullptr<Allocator>(),
                  std::forward<Args>(args)...);
  }

  /** Constructor. Initialize an already-allocated header. */
  template<typename ...Args>
  void shm_init(TYPED_HEADER &header,
                lipc::Allocator *alloc, Args&& ...args) {
    shm_destroy(false);
    shm_init_main(&header, alloc, std::forward<Args>(args)...);
  }

  /** Initialize the allocator */
  void shm_init_allocator(Allocator *alloc) {
    if (alloc == nullptr) {
      alloc_ = HERMES_SHM_MEMORY_MANAGER->GetDefaultAllocator();
    } else {
      alloc_ = alloc;
    }
  }

  /** Default initialize a data structure's header. */
  void shm_init_header(TYPED_HEADER *header,
                       Allocator *alloc) {
    if (IsValid()) { return; }
    shm_init_allocator(alloc);
    if (header != nullptr) {
      header_ = header;
      Allocator::ConstructObj<TYPED_HEADER>(*header_);
    } else {
      throw std::runtime_error("Header must be non-null during init of struct");
    }
  }

  /** Piecewise initialize a data structure's header. */
  template<typename ...Args>
  void shm_init_header(TYPED_HEADER *header,
                       Allocator *alloc,
                       Args&& ...args) {
    if (IsValid()) { return; }
    shm_init_allocator(alloc);
    if (header == nullptr) {
      throw std::runtime_error("Header must be non-null during init of struct");
    }
    header_ = header;
    Allocator::ConstructObj<TYPED_HEADER>(
      *header_,
      alloc_, std::forward<Args>(args)...);
    // TODO(llogan): pass headers to each container?
  }

  /**====================================
   * Serialization
   * ===================================*/

  /** Serialize into a Pointer */
  void shm_serialize(TypedPointer<TYPED_CLASS> &ar) const {
    ar = GetAllocator()->template
      Convert<TYPED_HEADER, Pointer>(header_);
    shm_serialize_main();
  }

  /** Serialize into an AtomicPointer */
  void shm_serialize(TypedAtomicPointer<TYPED_CLASS> &ar) const {
    ar = GetAllocator()->template
      Convert<TYPED_HEADER, AtomicPointer>(header_);
    shm_serialize_main();
  }

  /** Override << operators */
  SHM_SERIALIZE_OPS((TYPED_CLASS))

  /**====================================
   * Deserialization
   * ===================================*/

  /** Deserialize object from a raw pointer */
  bool shm_deserialize(const TypedPointer<TYPED_CLASS> &ar) {
    return shm_deserialize(
      HERMES_SHM_MEMORY_MANAGER->GetAllocator(ar.allocator_id_),
      ar.ToOffsetPointer()
    );
  }

  /** Deserialize object from allocator + offset */
  bool shm_deserialize(Allocator *alloc, OffsetPointer header_ptr) {
    if (header_ptr.IsNull()) { return false; }
    return shm_deserialize(alloc,
                           alloc->Convert<
                             TYPED_HEADER,
                             OffsetPointer>(header_ptr));
  }

  /** Deserialize object from another object (weak copy) */
  bool shm_deserialize(const CLASS_NAME &other) {
    if (other.IsNull()) { return false; }
    return shm_deserialize(other.GetAllocator(), other.header_);
  }

  /** Deserialize object from allocator + header */
  bool shm_deserialize(Allocator *alloc,
                       TYPED_HEADER *header) {
    /*alloc_ = alloc;
    header_ = header;
    hermes_shm::ForwardIterateTuple::Apply(
      objs_,
      [this, alloc](size_t i, auto &obj_) constexpr {
        if constexpr(IS_SHM_ARCHIVEABLE(decltype(obj_))) {
          obj_->shm_deserialize(alloc, this->header_->template Get<i>());
        }
      }
    );
    shm_deserialize_main();
    return true;*/
  }

  /** Constructor. Deserialize the object from the reference. */
  template<typename ...Args>
  void shm_init(lipc::ShmRef<TYPED_CLASS> &obj) {
    shm_deserialize(obj->GetAllocator(), obj->header_);
  }

  /** Override >> operators */
  SHM_DESERIALIZE_OPS ((TYPED_CLASS))

  /**====================================
   * Destructors
   * ===================================*/

  /** Destructor */
  ~CLASS_NAME() {
    shm_destroy(true);
  }

  /** Shm Destructor */
  void shm_destroy(bool destroy_header = true) {
    /*hermes_shm::ReverseIterateTuple::Apply(
      objs_,
      [destroy_header](size_t i, auto &obj_) constexpr {
        if constexpr(IS_SHM_ARCHIVEABLE(decltype(obj_))) {
          obj_.shm_destroy(destroy_header);
        }
      }
    );*/
  }

  /**====================================
   * Move Operations
   * ===================================*/

  /** Move constructor */
  CLASS_NAME(CLASS_NAME &&other) noexcept {
    shm_weak_move(
      typed_nullptr<TYPED_HEADER>(),
      typed_nullptr<Allocator>(),
      other);
  }

  /** Move assignment operator */
  CLASS_NAME& operator=(CLASS_NAME &&other) noexcept {
    shm_weak_move(
      typed_nullptr<TYPED_HEADER>(),
      typed_nullptr<Allocator>(),
      other);
    return *this;
  }

  /** Move shm_init constructor */
  void shm_init_main(TYPED_HEADER *header,
                     lipc::Allocator *alloc,
                     CLASS_NAME &&other) noexcept {
    shm_weak_move(header, alloc, other);
  }

  /** Move operation */
  void shm_weak_move(TYPED_HEADER *header,
                     lipc::Allocator *alloc,
                     CLASS_NAME &other) {
    /*hermes_shm::ForwardIterateTuple::Apply(
      objs_,
      [header, alloc, other](size_t i, auto &obj_) constexpr {
        if constexpr(IS_SHM_ARCHIVEABLE(decltype(obj_))) {
          obj_.shm_weak_move(header, alloc, other.objs_.template Get<i>());
        } else {
          obj_ = std::move(other.objs_.template Get<i>());
        }
      }
    );*/
  }

  /**====================================
   * Copy Operations
   * ===================================*/

  /** Copy constructor */
  CLASS_NAME(const CLASS_NAME &other) noexcept {
    shm_init(other);
  }

  /** Copy assignment constructor */
  CLASS_NAME &operator=(const CLASS_NAME &other) {
    if (this != &other) {
      shm_strong_copy(
        typed_nullptr<TYPED_HEADER >(),
        typed_nullptr<Allocator>(),
        other);
    }
    return *this;
  }

  /** Copy shm_init constructor */
  void shm_init_main(TYPED_HEADER *header,
                     lipc::Allocator *alloc,
                     const CLASS_NAME &other) {
    shm_strong_copy(header, alloc, other);
  }

  /** Strong Copy operation */
  void shm_strong_copy(TYPED_HEADER *header, lipc::Allocator *alloc,
                       const CLASS_NAME &other) {
    /*hermes_shm::ForwardIterateTuple::Apply(
      objs_,
      [header, alloc, other](size_t i, auto &obj_) constexpr {
        if constexpr(IS_SHM_ARCHIVEABLE(decltype(obj_))) {
          obj_.shm_strong_copy(header, alloc, other.objs_.template Get<i>());
        } else {
          obj_ = other.objs_.template Get<i>();
        }
      }
    );
    shm_strong_copy_main(header, alloc, other);*/
  }

  /**====================================
   * Container Flag Operations
   * ===================================*/

  /** Sets this object as destructable */
  void SetDestructable() {
    hermes_shm::ForwardIterateTuple::Apply(
      objs_,
      [](size_t i, auto &obj_) constexpr {
        if constexpr(IS_SHM_ARCHIVEABLE(decltype(obj_))) {
          obj_.SetDestructable();
        }
      }
    );
  }

  /** Sets this object as not destructable */
  void UnsetDestructable() {
    hermes_shm::ForwardIterateTuple::Apply(
      objs_,
      [](size_t i, auto &obj_) constexpr {
        if constexpr(IS_SHM_ARCHIVEABLE(decltype(obj_))) {
          obj_.UnsetDestructable();
        }
      }
    );
  }

  /** Check if this container is destructable */
  bool IsDestructable() const { return true; }

  /** Check if container has a valid header */
  bool IsValid() const { return header_ != nullptr; }

  /**====================================
   * Header Flag Operations
   * ===================================*/

  /** Check if null */
  bool IsNull() const {
    return IsValid();
  }

  /** Get a typed pointer to the object */
  template<typename POINTER_T>
  POINTER_T GetShmPointer() const {
    return GetAllocator()->template
      Convert<TYPED_HEADER, POINTER_T>(header_);
  }

  /**====================================
   * Query Operations
   * ===================================*/

  /** Get the allocator for this container */
  Allocator* GetAllocator() {
    return alloc_;
  }

  /** Get the allocator for this container */
  Allocator* GetAllocator() const {
    return alloc_;
  }

  /** Get the shared-memory allocator id */
  allocator_id_t GetAllocatorId() const {
    return GetAllocator()->GetId();
  }

  /** Get the ith constructed container in the tuple */
  template<size_t i>
  auto& Get() {
    return objs_.template Get<i>();
  }

  /** Get the ith constructed container in the tuple (const) */
  template<size_t i>
  auto& Get() const {
    return objs_.template Get<i>();
  }
};

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_STRUCT_H_
