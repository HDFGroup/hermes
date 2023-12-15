cmake_minimum_required(VERSION 3.10)
project(hermes)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------

add_executable(test_hermes_exec
        ${TEST_MAIN}/main_mpi.cc
        test_init.cc
        test_bucket.cc
)
add_dependencies(test_hermes_exec
        ${Hermes_CLIENT_DEPS} hermes)
target_link_libraries(test_hermes_exec
        ${Hermes_CLIENT_LIBRARIES} hermes Catch2::Catch2 MPI::MPI_CXX)
jarvis_test(test_hermes test_hermes)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------

# STRING TESTS
#add_test(NAME test_ipc COMMAND
#        ${CMAKE_BINARY_DIR}/bin/test_messages "TestIpc")

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_hermes_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_hermes_exec)
endif()
