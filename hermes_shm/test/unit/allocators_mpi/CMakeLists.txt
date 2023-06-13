cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------

add_executable(test_allocator_mpi_exec
        ${TEST_MAIN}/main_mpi.cc
        test_init.cc
        allocator_mpi.cc)
add_dependencies(test_allocator_mpi_exec hermes_shm_data_structures)
target_link_libraries(test_allocator_mpi_exec
        hermes_shm_data_structures Catch2::Catch2 MPI::MPI_CXX OpenMP::OpenMP_CXX)

#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------

# Multi-Thread ALLOCATOR tests
set(MP_ALLOCATORS
        StackAllocator
        ScalablePageAllocator)
foreach(ALLOCATOR ${MP_ALLOCATORS})
    add_test(NAME test_${ALLOCATOR}_mpi COMMAND
            mpirun -n 4 ${CMAKE_BINARY_DIR}/bin/test_allocator_mpi_exec "${ALLOCATOR}Mpi")
endforeach()

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_allocator_mpi_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_allocator_mpi_exec)
endif()