#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
        ${CMAKE_SOURCE_DIR}/adapter/include
)

find_package(Catch2 REQUIRED)

function(gcc exec args)
    message(INFO ${exec})
    add_test(NAME Test${exec} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" "-s" ${args})
endfunction()

function(mpi exec mpi_proc args)
    add_test(NAME Test${exec}_${mpi_proc}
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${mpi_proc}
            "${CMAKE_BINARY_DIR}/bin/${exec}" "-s" ${args})
endfunction()

#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_test hermes_stdio)
target_link_libraries(stdio_adapter_test Catch2::Catch2)
target_link_libraries(stdio_adapter_test -lstdc++fs -lc)
add_dependencies(stdio_adapter_test hermes_stdio)
gcc(stdio_adapter_test "")


add_executable(stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mpi_test hermes_stdio)
target_link_libraries(stdio_adapter_mpi_test Catch2::Catch2)
target_link_libraries(stdio_adapter_mpi_test -lstdc++fs -lc)
add_dependencies(stdio_adapter_mpi_test hermes_stdio)
target_link_libraries(stdio_adapter_mpi_test MPI::MPI_CXX)

mpi(stdio_adapter_mpi_test 2 "")

