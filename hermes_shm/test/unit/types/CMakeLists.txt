cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

add_executable(test_types
        ${TEST_MAIN}/main.cc
        test_init.cc
        test_argpack.cc
        test_util.cc)
add_dependencies(test_types hermes_shm_data_structures)
target_link_libraries(test_types
        hermes_shm_data_structures Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_types
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#------------------------------------------------------------------------------
# Code Coverage
#------------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_types)
endif()