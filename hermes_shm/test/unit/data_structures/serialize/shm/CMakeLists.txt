cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------
set (LIBS
        hermes_shm_data_structures
        Catch2::Catch2
        MPI::MPI_CXX
        OpenMP::OpenMP_CXX)
add_executable(test_shm_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_shm.cc)
add_dependencies(test_shm_exec
        hermes_shm_data_structures)
target_link_libraries(test_shm_exec ${LIBS})

add_test(NAME test_shm COMMAND ${CMAKE_BINARY_DIR}/bin/test_shm_exec)

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_shm_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})
