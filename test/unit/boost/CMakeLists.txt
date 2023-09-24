cmake_minimum_required(VERSION 3.10)
project(labstor)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------

add_executable(test_boost_exec
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_boost.cc
)
add_dependencies(test_boost_exec
        ${Labstor_RUNTIME_DEPS} hermes)
target_link_libraries(test_boost_exec
        ${Labstor_RUNTIME_LIBRARIES} Catch2::Catch2 MPI::MPI_CXX)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------

add_test(NAME test_boost COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_messages "TestBoost")

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_boost_exec
        EXPORT
        ${LABSTOR_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${LABSTOR_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(LABSTOR_ENABLE_COVERAGE)
    set_coverage_flags(test_boost_exec)
endif()
