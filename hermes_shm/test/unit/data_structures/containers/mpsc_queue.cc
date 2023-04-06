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

#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/ipc/mpsc_queue.h"

template<typename QueueT, typename T>
class QueueTestSuite {
 public:
  hipc::uptr<QueueT> &queue_;

 public:
  /** Constructor */
  explicit QueueTestSuite(hipc::uptr<QueueT> &queue)
  : queue_(queue) {}

  /** Producer method */
  void Produce(size_t count_per_rank) {
    try {
      int rank = omp_get_thread_num();
      for (size_t i = 0; i < count_per_rank; ++i) {
        size_t idx = rank * count_per_rank + i;
        CREATE_SET_VAR_TO_INT_OR_STRING(T, var, idx);
        queue_->emplace(var);
      }
    } catch (hshm::Error &e) {
      HELOG(kFatal, e.what())
    }
  }

  /** Consumer method */
  void Consume(size_t nproducers, size_t count_per_rank,
               bool inorder) {
    std::vector<int> entries;
    size_t total_size = nproducers * count_per_rank;
    entries.reserve(count_per_rank);
    auto entry = hipc::make_uptr<T>();
    auto entry_ref = hipc::to_ref(entry);

    // Consume everything
    while(entries.size() < total_size) {
      auto qtok = queue_->pop(entry_ref);
      if (qtok.IsNull()) {
        HERMES_THREAD_MODEL->Yield();
        continue;
      }
      CREATE_GET_INT_FROM_VAR(T, entry_int, *entry_ref)
      entries.emplace_back(entry_int);
    }

    // Ensure there's no data left in the queue
    REQUIRE(queue_->pop(entry_ref).IsNull());

    // Ensure that all elements are here
    if (!inorder) {
      std::sort(entries.begin(), entries.end());
    }
    for (int i = 0; i < (int)entries.size(); ++i) {
      REQUIRE(entries[i] == i);
    }
  }
};

template<typename QueueT, typename T>
void ProduceThenConsume(size_t nproducers, size_t count_per_rank) {
  size_t depth = 32;
  auto queue = hipc::make_uptr<QueueT>(depth);
  QueueTestSuite<QueueT, T> q(queue);

  // Produce all the data
  omp_set_dynamic(0);
#pragma omp parallel shared(nproducers, count_per_rank, q) num_threads(nproducers)  // NOLINT
  {  // NOLINT
#pragma omp barrier
    q.Produce(count_per_rank);
#pragma omp barrier
  }

  // Consume all the data
  q.Consume(nproducers, count_per_rank, nproducers == 1);
}

template<typename QueueT, typename T>
void ProduceAndConsume(size_t nproducers, size_t count_per_rank) {
  size_t depth = 32;
  auto queue = hipc::make_uptr<QueueT>(depth);
  size_t nthreads = nproducers + 1;
  QueueTestSuite<QueueT, T> q(queue);

  // Produce all the data
  omp_set_dynamic(0);
#pragma omp parallel shared(nproducers, count_per_rank, q) num_threads(nthreads)  // NOLINT
  {  // NOLINT
#pragma omp barrier
    size_t rank = omp_get_thread_num();
    if (rank < nproducers) {
      // Producer
      q.Produce(count_per_rank);
    } else {
      // Consumer
      q.Consume(nproducers, count_per_rank, false);
    }
#pragma omp barrier
  }
}

/**
 * TEST MPSC EXTENSIBLE QUEUE
 * */

TEST_CASE("TestMpscQueueExtInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue_ext<int>, int>(1, 32);
  ProduceThenConsume<hipc::mpsc_queue_ext<int>, int>(1, 64);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueExtString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue_ext<hipc::string>, hipc::string>(1, 32);
  ProduceThenConsume<hipc::mpsc_queue_ext<hipc::string>, hipc::string>(1, 64);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueExtIntMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue_ext<int>, int>(2, 16);
  ProduceThenConsume<hipc::mpsc_queue_ext<int>, int>(4, 64);
  ProduceThenConsume<hipc::mpsc_queue_ext<int>, int>(8, 256);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueExtStringMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue_ext<hipc::string>, hipc::string>(2, 16);
  ProduceThenConsume<hipc::mpsc_queue_ext<hipc::string>, hipc::string>(4, 64);
  ProduceThenConsume<hipc::mpsc_queue_ext<hipc::string>, hipc::string>(8, 256);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueExtStringMultiThreaded2") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceAndConsume<hipc::mpsc_queue_ext<hipc::string>, hipc::string>(8, 8192);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

/**
 * TEST MPSC QUEUE
 * */

TEST_CASE("TestMpscQueueInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue<int>, int>(1, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceThenConsume<hipc::mpsc_queue<hipc::string>, hipc::string>(1, 32);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueIntMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceAndConsume<hipc::mpsc_queue<int>, int>(8, 8192);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("TestMpscQueueStringMultiThreaded") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ProduceAndConsume<hipc::mpsc_queue<hipc::string>, hipc::string>(8, 8192);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}