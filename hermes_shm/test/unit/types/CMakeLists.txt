cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------
add_executable(test_types_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_argpack.cc
        test_util.cc)
add_dependencies(test_types_exec hermes_shm_data_structures)
target_link_libraries(test_types_exec
        hermes_shm_data_structures Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)

add_test(NAME test_types COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_types_exec "~[error=FatalError]")
add_test(NAME test_fatal_logger
        COMMAND ${CMAKE_BINARY_DIR}/bin/test_types_exec "[error=FatalError]")
set_tests_properties(
        test_fatal_logger PROPERTIES
        WILL_FAIL TRUE)

#-----------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_types_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_types_exec)
endif()