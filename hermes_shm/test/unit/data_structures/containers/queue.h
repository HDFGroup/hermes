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

#ifndef HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_QUEUE_H_
#define HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_QUEUE_H_

#include "hermes_shm/data_structures/data_structure.h"
#include "omp.h"
#include "hermes_shm/data_structures/ipc/ticket_stack.h"

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
    std::vector<size_t> idxs;
    int rank = omp_get_thread_num();
    try {
      for (size_t i = 0; i < count_per_rank; ++i) {
        size_t idx = rank * count_per_rank + i;
        CREATE_SET_VAR_TO_INT_OR_STRING(T, var, idx);
        CREATE_GET_INT_FROM_VAR(T, entry_int, var)
        idxs.emplace_back(entry_int);
        while (queue_->emplace(var).IsNull()) {}
      }
    } catch (hshm::Error &e) {
      HELOG(kFatal, e.what())
    }
    REQUIRE(idxs.size() == count_per_rank);
    std::sort(idxs.begin(), idxs.end());
    for (size_t i = 0; i < count_per_rank; ++i) {
      size_t idx = rank * count_per_rank + i;
      REQUIRE(idxs[i] == idx);
    }
  }

  /** Consumer method */
  void Consume(std::atomic<size_t> &count,
               size_t total_count,
               std::vector<size_t> &entries) {
    auto entry = hipc::make_uptr<T>();
    auto &entry_ref = *entry;

    // Consume everything
    while (count < total_count) {
      auto qtok = queue_->pop(entry_ref);
      if (qtok.IsNull()) {
        continue;
      }
      CREATE_GET_INT_FROM_VAR(T, entry_int, entry_ref)
      size_t off = count.fetch_add(1);
      if (off >= total_count) {
        break;
      }
      entries[off] = entry_int;
    }

    int rank = omp_get_thread_num();
    if (rank == 0) {
      // Ensure there's no data left in the queue
      REQUIRE(queue_->pop(entry_ref).IsNull());

      // Ensure the data is all correct
      REQUIRE(entries.size() == total_count);
      std::sort(entries.begin(), entries.end());
      REQUIRE(entries.size() == total_count);
      for (size_t i = 0; i < total_count; ++i) {
        REQUIRE(entries[i] == i);
      }
    }
  }
};

template<typename QueueT, typename T>
void ProduceThenConsume(size_t nproducers,
                        size_t nconsumers,
                        size_t count_per_rank,
                        size_t depth) {
  auto queue = hipc::make_uptr<QueueT>(depth);
  QueueTestSuite<QueueT, T> q(queue);
  std::atomic<size_t> count = 0;
  std::vector<size_t> entries;
  entries.resize(count_per_rank * nproducers);

  // Produce all the data
  omp_set_dynamic(0);
#pragma omp parallel shared(nproducers, count_per_rank, q, count, entries) num_threads(nproducers)  // NOLINT
  {  // NOLINT
#pragma omp barrier
    q.Produce(count_per_rank);
#pragma omp barrier
  }

  omp_set_dynamic(0);
#pragma omp parallel shared(nproducers, count_per_rank, q) num_threads(nconsumers)  // NOLINT
  {  // NOLINT
#pragma omp barrier
    // Consume all the data
    q.Consume(count, count_per_rank * nproducers, entries);
#pragma omp barrier
  }
}

template<typename QueueT, typename T>
void ProduceAndConsume(size_t nproducers,
                       size_t nconsumers,
                       size_t count_per_rank,
                       size_t depth) {
  auto queue = hipc::make_uptr<QueueT>(depth);
  size_t nthreads = nproducers + nconsumers;
  QueueTestSuite<QueueT, T> q(queue);
  std::atomic<size_t> count = 0;
  std::vector<size_t> entries;
  entries.resize(count_per_rank * nproducers);

  // Produce all the data
  omp_set_dynamic(0);
#pragma omp parallel shared(nproducers, count_per_rank, q, count) num_threads(nthreads)  // NOLINT
  {  // NOLINT
#pragma omp barrier
    size_t rank = omp_get_thread_num();
    if (rank < nproducers) {
      // Producer
      q.Produce(count_per_rank);
    } else {
      // Consumer
      q.Consume(count, count_per_rank * nproducers, entries);
    }
#pragma omp barrier
  }
}

#endif  // HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_QUEUE_H_
