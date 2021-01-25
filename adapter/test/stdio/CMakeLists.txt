function(gcc_hermes exec tag_name tags)
    add_test(NAME Test${exec}_${tag_name} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${tags})
    set_property(TEST Test${exec}_${tag_name}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST Test${exec}_${tag_name} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()

#------------------------------------------------------------------------------
# STDIO Adapter Internal tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_mapper_test stdio_adapter_mapper_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mapper_test
        hermes_stdio Catch2::Catch2 -lstdc++fs -ldl -lc MPI::MPI_CXX glog::glog)
add_dependencies(stdio_adapter_mapper_test hermes_stdio)
add_dependencies(stdio_adapter_mapper_test hermes_daemon)

#------------------------------------------------------------------------------
# STDIO Adapter End to End tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX glog::glog)
add_dependencies(stdio_adapter_test hermes_stdio)
add_dependencies(stdio_adapter_test hermes_daemon)

add_executable(hermes_stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_adapter_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX glog::glog)
target_link_libraries(hermes_stdio_adapter_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_test hermes_daemon)
gcc(stdio_adapter_test "")
gcc_hermes(hermes_stdio_adapter_test "" "")

add_executable(stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mpi_test Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX glog::glog)
add_dependencies(stdio_adapter_mpi_test hermes_stdio)
add_dependencies(stdio_adapter_mpi_test hermes_daemon)
mpi(stdio_adapter_mpi_test 2 "")

add_executable(hermes_stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_adapter_mpi_test Catch2::Catch2 -lstdc++fs -lc glog::glog MPI::MPI_CXX)
target_link_libraries(hermes_stdio_adapter_mpi_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_mpi_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_mpi_test hermes_daemon)

mpi_daemon(hermes_stdio_adapter_mpi_test 2 "" 1)



