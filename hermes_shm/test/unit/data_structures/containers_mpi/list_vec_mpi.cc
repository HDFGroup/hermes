//
// Created by lukemartinlogan on 1/30/23.
//

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
      for (lipc::ShmRef<T> var : obj) {
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
        lipc::ShmRef<T> first = *obj.begin();
        (*first) = update;
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    // Check if modification received
    {
      CREATE_SET_VAR_TO_INT_OR_STRING(T, update, count);
      lipc::ShmRef<T> first = *obj.begin();
      REQUIRE((*first) == update);
      MPI_Barrier(MPI_COMM_WORLD);
      MPI_Barrier(MPI_COMM_WORLD);
    }

  } catch(HERMES_SHM_ERROR_TYPE &HERMES_SHM_ERROR_PTR) {
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
  ListVecTest<int, lipc::list<int>>(100);
}

TEST_CASE("ListOfStringMpi") {
  ListVecTest<lipc::string, lipc::list<lipc::string>>(100);
}

TEST_CASE("VectorOfIntMpi") {
  ListVecTest<int, lipc::vector<int>>(100);
}

TEST_CASE("VectorOfStringMpi") {
  ListVecTest<lipc::string, lipc::vector<lipc::string>>(100);
}