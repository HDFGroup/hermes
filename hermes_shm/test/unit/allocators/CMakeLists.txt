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

# PAGE ALLOCATOR TESTS
add_test(NAME test_page_allocator COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_allocator_exec "StackAllocator")
add_test(NAME test_page_allocator_4t COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_allocator_exec "StackAllocatorMultithreaded")
