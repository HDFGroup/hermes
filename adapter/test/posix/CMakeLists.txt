include_directories(${CMAKE_SOURCE_DIR}/src/api)
include_directories(${CMAKE_SOURCE_DIR}/src)

#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------

add_executable(posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
add_dependencies(posix_adapter_test hermes)
target_link_libraries(posix_adapter_test hermes)
pytest(posix test_posix_basic)

add_executable(posix_adapter_mpi_test posix_adapter_mpi_test.cpp ${ADAPTER_COMMON})
pytest(posix test_posix_basic_mpi)

add_executable(hermes_posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_posix_adapter_test hermes_posix)
add_dependencies(hermes_posix_adapter_test hermes_posix hermes_daemon)
set_target_properties(hermes_posix_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
pytest(posix test_hermes_posix_basic_small)
pytest(posix test_hermes_posix_basic_large)

add_executable(hermes_posix_adapter_mpi_test posix_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_posix_adapter_mpi_test hermes_posix)
add_dependencies(hermes_posix_adapter_mpi_test hermes_posix hermes_daemon)
set_target_properties(hermes_posix_adapter_mpi_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
pytest(posix test_hermes_posix_basic_mpi_small)
pytest(posix test_hermes_posix_basic_mpi_large)

include_directories(${CMAKE_SOURCE_DIR}/adapter)
add_executable(posix_simple_io_mpi posix_simple_io_mpi.cc)
add_dependencies(posix_simple_io_mpi hermes_posix)
target_link_libraries(posix_simple_io_mpi hermes_posix)

add_executable(hermes_posix_simple_io_omp posix_simple_io_omp.cc)
add_dependencies(hermes_posix_simple_io_omp hermes_posix)
target_link_libraries(hermes_posix_simple_io_omp hermes_posix)

add_executable(posix_simple_io_omp posix_simple_io_omp.cc)
add_dependencies(posix_simple_io_omp hermes_shm_data_structures)
target_link_libraries(posix_simple_io_omp hermes_shm_data_structures)

pytest(posix test_hermes_posix_simple_io_omp_default)
pytest(posix test_hermes_posix_simple_io_omp_scratch)
pytest(posix test_hermes_posix_simple_io_omp_preload)

set(POSIX_TESTS
  posix_adapter_test
  hermes_posix_adapter_test
  posix_adapter_mpi_test
  hermes_posix_adapter_mpi_test
  posix_simple_io_mpi
  posix_simple_io_omp
)

foreach(program ${POSIX_TESTS})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_DIR})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_TEST_DIR})
  target_link_libraries(${program} Catch2::Catch2 -lstdc++fs -lc
          MPI::MPI_CXX OpenMP::OpenMP_CXX)
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
