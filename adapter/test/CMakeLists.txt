set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")
set(ADAPTER_COMMON ${CMAKE_CURRENT_SOURCE_DIR}/catch_config.h)
set(HERMES_ADAPTER_TEST_DIR ${HERMES_ADAPTER_DIR}/test)

find_package(Catch2 REQUIRED)
add_executable(hermes_daemon hermes_daemon.cc)
target_link_libraries(hermes_daemon -ldl -lc MPI::MPI_CXX)
target_link_libraries(hermes_daemon hermes)
add_dependencies(hermes_daemon hermes)

function(gcc exec args)
    add_test(NAME Test${exec} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${args})
    set_property(TEST Test${exec}
      PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
    set_property(TEST Test${exec} APPEND
      PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
endfunction()

function(mpi exec mpi_proc args)
    add_test(NAME Test${exec}_${mpi_proc}
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${mpi_proc}
            "${CMAKE_BINARY_DIR}/bin/${exec}" ${args} -d yes)
    set_property(TEST Test${exec}_${mpi_proc}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST Test${exec}_${mpi_proc} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()

find_program(BASH_PROGRAM bash)

function(mpi_daemon test_exec test_mpi_proc test_args arg_name daemon_procs)
    #MPI_EXEC=$1 TEST_EXEC=$2 TEST_PROCS=$3 HERMES_EXEC=$4 HERMES_PROCS=$5 HERMES_CONF=$6
    add_test(Test${test_exec}_${test_mpi_proc}_${arg_name} ${BASH_PROGRAM}
            ${CMAKE_SOURCE_DIR}/adapter/test/run_hermes.sh ${MPIEXEC_EXECUTABLE}
            ${CMAKE_BINARY_DIR}/bin/${test_exec} ${test_mpi_proc}
            ${CMAKE_BINARY_DIR}/bin/hermes_daemon ${daemon_procs}
            ${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf
            ${test_args})
    set_property(TEST Test${test_exec}_${test_mpi_proc}_${arg_name}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST Test${test_exec}_${test_mpi_proc}_${arg_name} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()

enable_testing()

if(HERMES_ENABLE_STDIO_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/stdio)
endif()

if(HERMES_ENABLE_POSIX_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/posix)
endif()

if(HERMES_ENABLE_MPIIO_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/mpiio)
endif()

if(HERMES_ENABLE_PUBSUB_ADAPTER)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/pubsub)
endif()

if(HERMES_ENABLE_VFD)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/vfd)
endif()

install(
        TARGETS
        hermes_daemon
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)
