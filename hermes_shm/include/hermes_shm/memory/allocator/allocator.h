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

#ifndef HERMES_MEMORY_ALLOCATOR_ALLOCATOR_H_
#define HERMES_MEMORY_ALLOCATOR_ALLOCATOR_H_

#include <cstdint>
#include <hermes_shm/memory/memory.h>
#include <hermes_shm/util/errors.h>

namespace hermes_shm::ipc {

/**
 * The allocator type.
 * Used to reconstruct allocator from shared memory
 * */
enum class AllocatorType {
  kStackAllocator,
  kMallocAllocator,
  kFixedPageAllocator,
  kScalablePageAllocator,
};

/**
 * The basic shared-memory allocator header.
 * Allocators inherit from this.
 * */
struct AllocatorHeader {
  int allocator_type_;
  allocator_id_t allocator_id_;
  size_t custom_header_size_;

  AllocatorHeader() = default;

  void Configure(allocator_id_t allocator_id,
                 AllocatorType type,
                 size_t custom_header_size) {
    allocator_type_ = static_cast<int>(type);
    allocator_id_ = allocator_id;
    custom_header_size_ = custom_header_size;
  }
};

/**
 * The allocator base class.
 * */
class Allocator {
 protected:
  char *buffer_;
  size_t buffer_size_;
  char *custom_header_;

 public:
  /**
   * Constructor
   * */
  Allocator() : custom_header_(nullptr) {}

  /**
   * Destructor
   * */
  virtual ~Allocator() = default;

  /**
   * Create the shared-memory allocator with \a id unique allocator id over
   * the particular slot of a memory backend.
   *
   * The shm_init function is required, but cannot be marked virtual as
   * each allocator has its own arguments to this method. Though each
   * allocator must have "id" as its first argument.
   * */
  // virtual void shm_init(allocator_id_t id, Args ...args) = 0;

  /**
   * Deserialize allocator from a buffer.
   * */
  virtual void shm_deserialize(char *buffer,
                               size_t buffer_size) = 0;

  /**
   * Allocate a region of memory of \a size size
   * */
  virtual OffsetPointer AllocateOffset(size_t size) = 0;

  /**
   * Allocate a region of memory to a specific pointer type
   * */
  template<typename POINTER_T=Pointer>
  POINTER_T Allocate(size_t size) {
    return POINTER_T(GetId(), AllocateOffset(size).load());
  }

  /**
   * Allocate a region of memory of \a size size
   * and \a alignment alignment. Assumes that
   * alignment is not 0.
   * */
  virtual OffsetPointer AlignedAllocateOffset(size_t size,
                                              size_t alignment) = 0;

  /**
   * Allocate a region of memory to a specific pointer type
   * */
  template<typename POINTER_T=Pointer>
  POINTER_T AlignedAllocate(size_t size, size_t alignment) {
    return POINTER_T(GetId(), AlignedAllocateOffset(size, alignment).load());
  }

  /**
   * Allocate a region of \a size size and \a alignment
   * alignment. Will fall back to regular Allocate if
   * alignmnet is 0.
   * */
  template<typename POINTER_T=Pointer>
  inline POINTER_T Allocate(size_t size, size_t alignment) {
    if (alignment == 0) {
      return Allocate<POINTER_T>(size);
    } else {
      return AlignedAllocate<POINTER_T>(size, alignment);
    }
  }

  /**
   * Reallocate \a pointer to \a new_size new size
   * If p is kNullPointer, will internally call Allocate.
   *
   * @return true if p was modified.
   * */
  template<typename POINTER_T=Pointer>
  inline bool Reallocate(POINTER_T &p, size_t new_size) {
    if (p.IsNull()) {
      p = Allocate<POINTER_T>(new_size);
      return true;
    }
    auto new_p = ReallocateOffsetNoNullCheck(p.ToOffsetPointer(),
                                             new_size);
    bool ret = new_p == p.ToOffsetPointer();
    p.off_ = new_p.load();
    return ret;
  }

  /**
   * Reallocate \a pointer to \a new_size new size.
   * Assumes that p is not kNullPointer.
   *
   * @return true if p was modified.
   * */
  virtual OffsetPointer ReallocateOffsetNoNullCheck(OffsetPointer p,
                                                    size_t new_size) = 0;

  /**
   * Free the memory pointed to by \a ptr Pointer
   * */
  template<typename T=void>
  inline void FreePtr(T *ptr) {
    if (ptr == nullptr) {
      throw INVALID_FREE.format();
    }
    FreeOffsetNoNullCheck(Convert<T, OffsetPointer>(ptr));
  }

  /**
   * Free the memory pointed to by \a p Pointer
   * */
  template<typename POINTER_T=Pointer>
  inline void Free(POINTER_T &p) {
    if (p.IsNull()) {
      throw INVALID_FREE.format();
    }
    FreeOffsetNoNullCheck(OffsetPointer(p.off_.load()));
  }

  /**
   * Free the memory pointed to by \a ptr Pointer
   * */
  virtual void FreeOffsetNoNullCheck(OffsetPointer p) = 0;

  /**
   * Get the allocator identifier
   * */
  virtual allocator_id_t GetId() = 0;

  /**
   * Get the amount of memory that was allocated, but not yet freed.
   * Useful for memory leak checks.
   * */
  virtual size_t GetCurrentlyAllocatedSize() = 0;


  ///////////////////////////////////////
  /////////// POINTER ALLOCATORS
  ///////////////////////////////////////

  /**
   * Allocate a pointer of \a size size and return \a p process-independent
   * pointer and a process-specific pointer.
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* AllocatePtr(size_t size, POINTER_T &p, size_t alignment = 0) {
    p = Allocate<POINTER_T>(size, alignment);
    if (p.IsNull()) { return nullptr; }
    return reinterpret_cast<T*>(buffer_ + p.off_.load());
  }

  /**
   * Allocate a pointer of \a size size
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* AllocatePtr(size_t size, size_t alignment = 0) {
    POINTER_T p;
    return AllocatePtr<T, POINTER_T>(size, p, alignment);
  }

  /**
   * Allocate a pointer of \a size size
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* ClearAllocatePtr(size_t size, size_t alignment = 0) {
    POINTER_T p;
    return ClearAllocatePtr<T, POINTER_T>(size, p, alignment);
  }

  /**
   * Allocate a pointer of \a size size and return \a p process-independent
   * pointer and a process-specific pointer.
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* ClearAllocatePtr(size_t size, POINTER_T &p, size_t alignment = 0) {
    p = Allocate<POINTER_T>(size, alignment);
    if (p.IsNull()) { return nullptr; }
    auto ptr = reinterpret_cast<T*>(buffer_ + p.off_.load());
    if (ptr) {
      memset(ptr, 0, size);
    }
    return ptr;
  }

  /**
   * Reallocate a pointer to a new size
   *
   * @param p process-independent pointer (input & output)
   * @param new_size the new size to allocate
   * @param modified whether or not p was modified (output)
   * @return A process-specific pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* ReallocatePtr(POINTER_T &p, size_t new_size, bool &modified) {
    modified = Reallocate<POINTER_T>(p, new_size);
    return Convert<T>(p);
  }

  /**
   * Reallocate a pointer to a new size
   *
   * @param p process-independent pointer (input & output)
   * @param new_size the new size to allocate
   * @return A process-specific pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* ReallocatePtr(POINTER_T &p, size_t new_size) {
    Reallocate<POINTER_T>(p, new_size);
    return Convert<T>(p);
  }

  /**
   * Reallocate a pointer to a new size
   *
   * @param old_ptr process-specific pointer to reallocate
   * @param new_size the new size to allocate
   * @return A process-specific pointer
   * */
  template<typename T>
  inline T* ReallocatePtr(T *old_ptr, size_t new_size) {
    OffsetPointer p = Convert<T, OffsetPointer>(old_ptr);
    return ReallocatePtr<T, OffsetPointer>(p, new_size);
  }

  ///////////////////////////////////////
  ///////////OBJECT ALLOCATORS
  ///////////////////////////////////////

  /**
   * Allocate an array of objects (but don't construct).
   *
   * @return A process-specific pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* AllocateObjs(size_t count) {
    POINTER_T p;
    return AllocateObjs<T>(count, p);
  }

  /**
   * Allocate an array of objects (but don't construct).
   *
   * @param count the number of objects to allocate
   * @param p process-independent pointer (output)
   * @return A process-specific pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* AllocateObjs(size_t count, POINTER_T &p) {
    return AllocatePtr<T>(count * sizeof(T), p);
  }

  /**
   * Allocate an array of objects and memset to 0.
   *
   * @param count the number of objects to allocate
   * @param p process-independent pointer (output)
   * @return A process-specific pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* ClearAllocateObjs(size_t count, POINTER_T &p) {
    return ClearAllocatePtr<T>(count * sizeof(T), p);
  }

  /**
   * Allocate and construct an array of objects
   *
   * @param count the number of objects to allocate
   * @param p process-independent pointer (output)
   * @param args parameters to construct object of type T
   * @return A process-specific pointer
   * */
  template<
    typename T,
    typename POINTER_T=Pointer,
    typename ...Args>
  inline T* AllocateConstructObjs(size_t count, POINTER_T &p, Args&& ...args) {
    T *ptr = AllocateObjs<T>(count, p);
    ConstructObjs<T>(ptr, 0, count, std::forward<Args>(args)...);
    return ptr;
  }

  /**
   * Reallocate a pointer of objects to a new size.
   *
   * @param p process-independent pointer (input & output)
   * @param old_count the original number of objects (avoids reconstruction)
   * @param new_count the new number of objects
   *
   * @return A process-specific pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* ReallocateObjs(POINTER_T &p, size_t new_count) {
    T *ptr = ReallocatePtr<T>(p, new_count*sizeof(T));
    return ptr;
  }

  /**
   * Reallocate a pointer of objects to a new size and construct the
   * new elements in-place.
   *
   * @param p process-independent pointer (input & output)
   * @param old_count the original number of objects (avoids reconstruction)
   * @param new_count the new number of objects
   * @param args parameters to construct object of type T
   *
   * @return A process-specific pointer
   * */
  template<
    typename T,
    typename POINTER_T=Pointer,
    typename ...Args>
  inline T* ReallocateConstructObjs(POINTER_T &p,
                                    size_t old_count,
                                    size_t new_count,
                                    Args&& ...args) {
    T *ptr = ReallocatePtr<T>(p, new_count*sizeof(T));
    ConstructObjs<T>(ptr, old_count, new_count, std::forward<Args>(args)...);
    return ptr;
  }

  /**
   * Construct each object in an array of objects.
   *
   * @param ptr the array of objects (potentially archived)
   * @param old_count the original size of the ptr
   * @param new_count the new size of the ptr
   * @param args parameters to construct object of type T
   * @return None
   * */
  template<
    typename T,
    typename ...Args>
  inline static void ConstructObjs(T *ptr,
                            size_t old_count,
                            size_t new_count, Args&& ...args) {
    if (ptr == nullptr) { return; }
    for (size_t i = old_count; i < new_count; ++i) {
      ConstructObj<T>(*(ptr + i), std::forward<Args>(args)...);
    }
  }

  /**
   * Construct an object.
   *
   * @param ptr the object to construct (potentially archived)
   * @param args parameters to construct object of type T
   * @return None
   * */
  template<
    typename T,
    typename ...Args>
  inline static void ConstructObj(T &obj, Args&& ...args) {
    new (&obj) T(std::forward<Args>(args)...);
  }

  /**
   * Destruct an array of objects
   *
   * @param ptr the object to destruct (potentially archived)
   * @param count the length of the object array
   * @return None
   * */
  template<typename T>
  inline static void DestructObjs(T *ptr, size_t count) {
    if (ptr == nullptr) { return; }
    for (size_t i = 0; i < count; ++i) {
      DestructObj<T>((ptr + i));
    }
  }

  /**
   * Destruct an object
   *
   * @param ptr the object to destruct (potentially archived)
   * @param count the length of the object array
   * @return None
   * */
  template<typename T>
  inline static void DestructObj(T &obj) {
    obj.~T();
  }

  /**
   * Get the custom header of the shared-memory allocator
   *
   * @return Custom header pointer
   * */
  template<typename HEADER_T>
  inline HEADER_T* GetCustomHeader() {
    return reinterpret_cast<HEADER_T*>(custom_header_);
  }

  /**
   * Convert a process-independent pointer into a process-specific pointer
   *
   * @param p process-independent pointer
   * @return a process-specific pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline T* Convert(const POINTER_T &p) {
    if (p.IsNull()) { return nullptr; }
    return reinterpret_cast<T*>(buffer_ + p.off_.load());
  }

  /**
   * Convert a process-specific pointer into a process-independent pointer
   *
   * @param ptr process-specific pointer
   * @return a process-independent pointer
   * */
  template<typename T, typename POINTER_T=Pointer>
  inline POINTER_T Convert(T *ptr) {
    if (ptr == nullptr) { return POINTER_T::GetNull(); }
    return POINTER_T(GetId(),
                     reinterpret_cast<size_t>(ptr) -
                     reinterpret_cast<size_t>(buffer_));
  }

  /**
   * Determine whether or not this allocator contains a process-specific
   * pointer
   *
   * @param ptr process-specific pointer
   * @return True or false
   * */
  template<typename T = void>
  inline bool ContainsPtr(T *ptr) {
    return  reinterpret_cast<size_t>(ptr) >=
            reinterpret_cast<size_t>(buffer_);
  }
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_MEMORY_ALLOCATOR_ALLOCATOR_H_
