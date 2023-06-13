cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------
add_executable(test_thread_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_thread.cc
        test_lock.cc)
add_dependencies(test_thread_exec hermes_shm_data_structures)
target_link_libraries(test_thread_exec
        hermes_shm_data_structures
        thallium
        Catch2::Catch2
        MPI::MPI_CXX
        OpenMP::OpenMP_CXX)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------
add_test(NAME test_thread COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_thread_exec)

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_thread_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_thread_exec)
endif()