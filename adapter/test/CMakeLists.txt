set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(ADAPTER_COMMON ${CMAKE_CURRENT_SOURCE_DIR}/catch_config.h)
#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
        ${CMAKE_SOURCE_DIR}/adapter/include
        ${CMAKE_SOURCE_DIR}/adapter/src)

find_package(Catch2 REQUIRED)

function(gcc exec args)
    add_test(NAME Test${exec} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${args})
endfunction()

function(mpi exec mpi_proc args)
    add_test(NAME Test${exec}_${mpi_proc}
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${mpi_proc}
            "${CMAKE_BINARY_DIR}/bin/${exec}" ${args})
endfunction()


add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/posix)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/stdio)