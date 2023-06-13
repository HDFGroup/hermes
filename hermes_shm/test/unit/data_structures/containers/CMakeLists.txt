cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------

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
        mpsc_queue.cc
        spsc_queue.cc
        charbuf.cc
        ticket_queue.cc
)

add_dependencies(test_data_structure_exec hermes_shm_data_structures)
target_link_libraries(test_data_structure_exec
        hermes_shm_data_structures Catch2::Catch2
        MPI::MPI_CXX OpenMP::OpenMP_CXX)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------

# STRING TESTS
add_test(NAME test_string COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "String")

# VECTOR TESTS
add_test(NAME test_vector COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "Vector*")

# LIST TESTS
add_test(NAME test_list COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "List*")

# SLIST TESTS
add_test(NAME test_slist COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "Slist*")

# MANUAL PTR TESTS
add_test(NAME test_manual_ptr COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "ManualPtr*")

# UNIQUE PTR TESTS
add_test(NAME test_unique_ptr COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "UniquePtr*")

# UNORDERED_MAP TESTS
add_test(NAME test_unordered_map COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "UnorderedMap*")

# PAIR TESTS
add_test(NAME test_pair COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "Pair*")

# IQUEUE TESTS
add_test(NAME test_iqueue COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "IqueueOfMpPage")

# SPSC TESTS
add_test(NAME test_spsc COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "TestSpsc*")

# MPSC TESTS
add_test(NAME test_mpsc COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "TestMpsc*")

# TicketQueue TESTS
add_test(NAME test_tkt_queue COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_data_structure_exec "TestTicket*")

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_data_structure_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_data_structure_exec)
endif()
