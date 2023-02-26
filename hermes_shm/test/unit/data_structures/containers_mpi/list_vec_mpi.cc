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
#include "hermes_shm/data_structures/string.h"
#include "hermes_shm/data_structures/thread_unsafe/list.h"
#include "hermes_shm/data_structures/thread_unsafe/vector.h"
#include "hermes_shm/util/error.h"

template<typename T, typename ContainerT>
void ListVecTest(size_t count) {
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  Allocator *alloc = alloc_g;
  Pointer *header = alloc->GetCustomHeader<Pointer>();
  ContainerT obj;

  try {
    if (rank == 0) {
      obj.shm_init(alloc);
      obj >> (*header);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    obj.shm_deserialize(*header);
    MPI_Barrier(MPI_COMM_WORLD);

    // Write 100 objects from rank 0
    {
      if (rank == 0) {
        for (int i = 0; i < count; ++i) {
          CREATE_SET_VAR_TO_INT_OR_STRING(T, var, i);
          obj.emplace_back(var);
        }
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    // Read 100 objects from every rank
    {
      REQUIRE(obj.size() == count);
      int i = 0;
      for (hipc::ShmRef<T> var : obj) {
        CREATE_SET_VAR_TO_INT_OR_STRING(T, orig, i);
        REQUIRE(*var == orig);
        ++i;
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    // Modify an object in rank 0
    {
      if (rank == 0) {
        CREATE_SET_VAR_TO_INT_OR_STRING(T, update, count);
        hipc::ShmRef<T> first = *obj.begin();
        (*first) = update;
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    // Check if modification received
    {
      CREATE_SET_VAR_TO_INT_OR_STRING(T, update, count);
      hipc::ShmRef<T> first = *obj.begin();
      REQUIRE((*first) == update);
      MPI_Barrier(MPI_COMM_WORLD);
      MPI_Barrier(MPI_COMM_WORLD);
    }

  } catch(HERMES_ERROR_TYPE &HERMES_ERROR_PTR) {
    std::cout << "HERE0" << std::endl;
    err->print();
  } catch(hermes_shm::Error &err) {
    std::cout << "HERE1" << std::endl;
    err.print();
  } catch(int err) {
    std::cout << "HERE2" << std::endl;
  } catch(std::runtime_error &err) {
    std::cout << "HERE3" << std::endl;
  } catch(std::logic_error &err) {
    std::cout << "HERE4" << std::endl;
  } catch(...) {
    std::cout << "HERE5" << std::endl;
  }
}

TEST_CASE("ListOfIntMpi") {
  ListVecTest<int, hipc::list<int>>(100);
}

TEST_CASE("ListOfStringMpi") {
  ListVecTest<hipc::string, hipc::list<hipc::string>>(100);
}

TEST_CASE("VectorOfIntMpi") {
  ListVecTest<int, hipc::vector<int>>(100);
}

TEST_CASE("VectorOfStringMpi") {
  ListVecTest<hipc::string, hipc::vector<hipc::string>>(100);
}
