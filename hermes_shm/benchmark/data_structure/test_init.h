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

#ifndef HERMES_BENCHMARK_DATA_STRUCTURE_TEST_INIT_H_
#define HERMES_BENCHMARK_DATA_STRUCTURE_TEST_INIT_H_

#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>

#include <boost/container/scoped_allocator.hpp>

#include "hermes_shm/data_structures/data_structure.h"
#include <hermes_shm/util/timer.h>
#include <hermes_shm/util/type_switch.h>

using hshm::ipc::MemoryBackendType;
using hshm::ipc::MemoryBackend;
using hshm::ipc::allocator_id_t;
using hshm::ipc::AllocatorType;
using hshm::ipc::Allocator;
using hshm::ipc::Pointer;

using hshm::ipc::MemoryBackendType;
using hshm::ipc::MemoryBackend;
using hshm::ipc::allocator_id_t;
using hshm::ipc::AllocatorType;
using hshm::ipc::Allocator;
using hshm::ipc::MemoryManager;
using hshm::ipc::Pointer;

namespace bipc = boost::interprocess;

/** Shared memory segment (a large contiguous region) */
typedef bipc::managed_shared_memory::segment_manager segment_manager_t;

/** Create boost segment singleton */
struct BoostSegment {
  std::unique_ptr<bipc::managed_shared_memory> segment_;

  BoostSegment() {
    bipc::shared_memory_object::remove("HermesBoostBench");
    segment_ = std::make_unique<bipc::managed_shared_memory>(
      bipc::create_only, "HermesBoostBench", GIGABYTES(4));
  }
};
#define BOOST_SEGMENT \
  hshm::EasySingleton<BoostSegment>::GetInstance()->segment_.get()

/** Create boost allocator singleton */
template<typename T>
struct BoostAllocator {
  typedef bipc::allocator<T, segment_manager_t> alloc_t;
  alloc_t alloc_;

  BoostAllocator()
  : alloc_(BOOST_SEGMENT->get_segment_manager()) {}
};
#define BOOST_ALLOCATOR(T) \
  hshm::EasySingleton<BoostAllocator<TYPE_UNWRAP(T)>>::GetInstance()->alloc_

/** A generic string using allocator */
typedef boost::interprocess::basic_string<
  char, std::char_traits<char>,
  typename BoostAllocator<char>::alloc_t> bipc_string;

/** Instance of the segment */
extern std::unique_ptr<bipc::managed_shared_memory> segment_g;

/**
 * Converts an arbitrary type to std::string or int
 * */
template<typename T>
struct StringOrInt {
  typedef typename hshm::type_switch<T, size_t,
                                     size_t, size_t,
                                     std::string, std::string,
                                     hipc::string, hipc::uptr<hipc::string>,
                                     bipc_string, bipc_string*>::type
    internal_t;
  internal_t internal_;

  /** Convert from int to internal_t */
  StringOrInt(size_t num) {
    if constexpr(std::is_same_v<T, size_t>) {
      internal_ = num;
    } else if constexpr(std::is_same_v<T, std::string>) {
      internal_ = std::to_string(num);
    } else if constexpr(std::is_same_v<T, hipc::string>) {
      internal_ = hipc::make_uptr<hipc::string>(std::to_string(num));
    } else if constexpr(std::is_same_v<T, bipc_string>) {
      internal_ = BOOST_SEGMENT->find_or_construct<bipc_string>("MyString")(
        BOOST_ALLOCATOR(bipc_string));
    }
  }

  /** Get the internal type */
  T& Get() {
    if constexpr(std::is_same_v<T, size_t>) {
      return internal_;
    } else if constexpr(std::is_same_v<T, std::string>) {
      return internal_;
    } else if constexpr(std::is_same_v<T, hipc::string>) {
      return *internal_;
    } else if constexpr(std::is_same_v<T, bipc_string>) {
      return *internal_;
    }
  }

  /** Convert internal_t to int */
  size_t ToInt() {
    if constexpr(std::is_same_v<T, size_t>) {
      return internal_;
    } else {
      size_t num;
      std::stringstream(ToString(internal_)) >> num;
      return num;
    }
  }

  /** Convert internal_t to std::string */
  std::string ToString() {
    if constexpr(std::is_same_v<T, size_t>) {
      return std::to_string(internal_);
    } else if constexpr(std::is_same_v<T, std::string>) {
      return internal_;
    } else if constexpr(std::is_same_v<T, hipc::string>) {
      return internal_->str();
    } else if constexpr(std::is_same_v<T, bipc_string>) {
      return std::string(internal_->c_str(), internal_->length());
    }
  }

  /** Destructor */
  ~StringOrInt() {
    if constexpr(std::is_same_v<T, size_t>) {
    } else if constexpr(std::is_same_v<T, std::string>) {
    } else if constexpr(std::is_same_v<T, hipc::string>) {
    } else if constexpr(std::is_same_v<T, bipc_string>) {
      BOOST_SEGMENT->destroy<bipc_string>("MyString");
    }
  }
};

/**
 * Gets the semantic name of a type
 * */
template<typename T>
struct InternalTypeName {
  static std::string Get() {
    if constexpr(std::is_same_v<T, hipc::string>) {
      return "hipc::string";
    } else if constexpr(std::is_same_v<T, std::string>) {
      return "std::string";
    } else if constexpr(std::is_same_v<T, bipc_string>) {
      return "bipc::string";
    } else if constexpr(std::is_same_v<T, size_t>) {
      return "int";
    }
  }
};


/** Timer */
using Timer = hshm::HighResMonotonicTimer;

/** Avoid compiler warning */
#define USE(var) ptr_ = (void*)&(var);

#endif //HERMES_BENCHMARK_DATA_STRUCTURE_TEST_INIT_H_
