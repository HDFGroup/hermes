cmake_minimum_required(VERSION 3.10)
project(labstor)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------

add_executable(test_performance_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_latency.cc)
add_dependencies(test_performance_exec
        ${Labstor_RUNTIME_DEPS} hermes)
target_link_libraries(test_performance_exec
        ${Labstor_RUNTIME_LIBRARIES} hermes Catch2::Catch2
        MPI::MPI_CXX)

#add_executable(test_performance_exec
#        ${TEST_MAIN}/main.cc
#        test_init.cc
#        test_latency.cc
#        test_zmq.cc
#)
#add_dependencies(test_performance_exec
#        ${Labstor_CLIENT_DEPS} hermes)
#target_link_libraries(test_performance_exec
#        ${Labstor_CLIENT_LIBRARIES} hermes Catch2::Catch2
#        MPI::MPI_CXX ${ZMQ_LIBRARIES})

add_executable(test_hermes_api
        hermes_api_bench.cc)
add_dependencies(test_hermes_api
        ${Labstor_CLIENT_DEPS} hermes)
target_link_libraries(test_hermes_api
        ${Labstor_CLIENT_LIBRARIES} hermes
        Catch2::Catch2 MPI::MPI_CXX)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------

# STRING TESTS
add_test(NAME test_performance COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_performance_exec "TestIpc")

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_performance_exec
        test_hermes_api
        EXPORT
        ${LABSTOR_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${LABSTOR_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(LABSTOR_ENABLE_COVERAGE)
    set_coverage_flags(test_performance_exec)
    set_coverage_flags(test_hermes_api)
endif()
