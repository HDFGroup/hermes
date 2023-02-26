cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

add_executable(test_data_structure_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        string.cc
        pair.cc
        #tuple.cc
        list.cc
        slist.cc
        vector.cc
        iqueue.cc
        manual_ptr.cc
        unique_ptr.cc
        unordered_map.cc
        #unordered_map_thread.cc
)

add_dependencies(test_data_structure_exec hermes_shm_data_structures)
target_link_libraries(test_data_structure_exec
        hermes_shm_data_structures Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)

# STRING TESTS
add_test(NAME test_string COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "String")

# VECTOR TESTS
add_test(NAME test_vector_of_int COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "VectorOfInt")
add_test(NAME test_vector_of_string COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "VectorOfString")
add_test(NAME test_vector_of_list_of_string COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "VectorOfListOfString")

# LIST TESTS
add_test(NAME test_list_of_int COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "ListOfInt")
add_test(NAME test_list_of_string COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "ListOfString")

# MANUAL PTR TESTS
add_test(NAME test_manual_ptr_of_string COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "ManualPtrOfString")

# UNIQUE PTR TESTS
add_test(NAME test_unique_ptr_of_string COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "UniquePtrOfString")

# UNORDERED_MAP TESTS
add_test(NAME test_unordered_map_of_int_int COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "UnorderedMapOfIntInt")
add_test(NAME test_unordered_map_of_int_str COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "UnorderedMapOfIntString")
add_test(NAME test_unordered_map_of_str_int COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "UnorderedMapOfStringInt")
add_test(NAME test_unordered_map_of_str_str COMMAND
        ${CMAKE_CURRENT_BINARY_DIR}/test_data_structure_exec "UnorderedMapOfStringString")
