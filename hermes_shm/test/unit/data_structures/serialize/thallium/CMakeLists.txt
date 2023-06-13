cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------
set (LIBS
        hermes_shm_data_structures
        Catch2::Catch2
        MPI::MPI_CXX
        OpenMP::OpenMP_CXX
        thallium)

add_executable(test_thallium_server server.cc)
add_dependencies(test_thallium_server
        hermes_shm_data_structures)
target_link_libraries(test_thallium_server ${LIBS})

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------
add_executable(test_thallium_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_thallium.cc)
add_dependencies(test_thallium_exec
        hermes_shm_data_structures)
target_link_libraries(test_thallium_exec ${LIBS})

add_test(NAME test_thallium COMMAND
        bash ${CMAKE_CURRENT_SOURCE_DIR}/test_thallium.sh
        "${CMAKE_BINARY_DIR}")

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_thallium_server
        test_thallium_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_thallium_server)
    set_coverage_flags(test_thallium_exec)
endif()