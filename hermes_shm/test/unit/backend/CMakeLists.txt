cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#------------------------------------------------------------------------------
# Build Tests
#------------------------------------------------------------------------------

add_executable(test_memory_exec
        ${TEST_MAIN}/main_mpi.cc
        test_init.cc
        backend.cc
        memory_slots.cc
        memory_manager.cc)
add_dependencies(test_memory_exec hermes_shm_data_structures)
target_link_libraries(test_memory_exec
        hermes_shm_data_structures Catch2::Catch2 MPI::MPI_CXX)
#------------------------------------------------------------------------------
# Test Cases
#------------------------------------------------------------------------------

add_test(NAME test_memory_slots COMMAND
        mpirun -n 2 ${CMAKE_BINARY_DIR}/bin/test_memory_exec "MemorySlot")
add_test(NAME test_reserve COMMAND
        ${CMAKE_BINARY_DIR}/bin/test_memory_exec "BackendReserve")
add_test(NAME test_memory_manager COMMAND
        mpirun -n 2 ${CMAKE_BINARY_DIR}/bin/test_memory_exec "MemoryManager")

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        test_memory_exec
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(test_memory_exec)
endif()