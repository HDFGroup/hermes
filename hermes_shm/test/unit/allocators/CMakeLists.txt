cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

add_executable(test_allocator_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        allocator.cc
        allocator_thread.cc)
add_dependencies(test_allocator_exec hermes_shm_data_structures)
target_link_libraries(test_allocator_exec
        hermes_shm_data_structures Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)

# ALLOCATOR tests
set(ALLOCATORS
        StackAllocator
        MallocAllocator
        ScalablePageAllocator)
foreach(ALLOCATOR ${ALLOCATORS})
    add_test(NAME test_${ALLOCATOR} COMMAND
            ${CMAKE_CURRENT_BINARY_DIR}/test_allocator_exec "${ALLOCATOR}")
endforeach()

# Multi-Thread ALLOCATOR tests
set(MT_ALLOCATORS
        StackAllocator
        ScalablePageAllocator)
foreach(ALLOCATOR ${MT_ALLOCATORS})
    add_test(NAME test_${ALLOCATOR}_4t COMMAND
            ${CMAKE_CURRENT_BINARY_DIR}/test_allocator_exec "${ALLOCATOR}Multithreaded")
endforeach()