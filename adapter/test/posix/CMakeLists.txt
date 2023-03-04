include_directories(${CMAKE_SOURCE_DIR}/src/api)
include_directories(${CMAKE_SOURCE_DIR}/src)

#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------

add_executable(posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
gcc(posix_adapter_test "")

add_executable(posix_adapter_mpi_test posix_adapter_mpi_test.cpp ${ADAPTER_COMMON})
mpi(posix_adapter_mpi_test 2 "")

add_executable(hermes_posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_posix_adapter_test hermes_posix yaml-cpp)
add_dependencies(hermes_posix_adapter_test hermes_posix hermes_daemon)
set_target_properties(hermes_posix_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
gcc_hermes(hermes_posix_adapter_test "" "~[request_size=range-large]" hermes 0)
gcc_hermes(hermes_posix_adapter_test "large" "[request_size=range-large]" hermes 0)

add_executable(hermes_posix_adapter_mpi_test posix_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_posix_adapter_mpi_test hermes_posix)
add_dependencies(hermes_posix_adapter_mpi_test hermes_posix hermes_daemon)
set_target_properties(hermes_posix_adapter_mpi_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
mpi_daemon(hermes_posix_adapter_mpi_test 2 "~[request_size=range-large]" "" 1)
mpi_daemon(hermes_posix_adapter_mpi_test 2 "[request_size=range-large]" "large" 1)

include_directories(${CMAKE_SOURCE_DIR}/adapter)
add_executable(posix_simple_io_mpi posix_simple_io_mpi.cc)
add_dependencies(posix_simple_io_mpi hermes_posix)
target_link_libraries(posix_simple_io_mpi hermes_posix)

add_executable(posix_simple_io_omp posix_simple_io_omp.cc)
add_dependencies(posix_simple_io_omp hermes_posix)
target_link_libraries(posix_simple_io_omp hermes_posix)

set(POSIX_TESTS
  posix_adapter_test
  hermes_posix_adapter_test
  posix_adapter_mpi_test
  hermes_posix_adapter_mpi_test
  posix_simple_io_mpi
)

foreach(program ${POSIX_TESTS})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_DIR})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_TEST_DIR})
  target_link_libraries(${program} Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
endforeach()

if(HERMES_INSTALL_TESTS)
    foreach(program ${POSIX_TESTS})
        install(
                TARGETS
                ${program}
                LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
                ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
                RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
    endforeach()
endif()

preload_hermes(python hermes_posix
        "python3 ${CMAKE_CURRENT_SOURCE_DIR}/hdf5_write_read.py
        /tmp/test_python.txt")

preload_hermes(simple_ior hermes_posix "ior -w -r -o /tmp/test_ior.txt")