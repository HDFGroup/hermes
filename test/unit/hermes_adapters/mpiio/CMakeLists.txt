# MPI adapter tests without hermes
add_executable(mpiio_adapter_test
        mpiio_adapter_test.cc
        mpiio_adapter_basic_test.cc)
target_link_libraries(mpiio_adapter_test
        hermes)
add_dependencies(mpiio_adapter_test
        hermes)
target_compile_definitions(mpiio_adapter_test PUBLIC
        HERMES_MPI_TESTS=true)
jarvis_test(mpiio test_mpiio_basic)

# MPI adapter tests with hermes
add_executable(hermes_mpiio_adapter_test
        mpiio_adapter_test.cc
        mpiio_adapter_basic_test.cc)
target_link_libraries(hermes_mpiio_adapter_test
        hermes_mpiio)
add_dependencies(hermes_mpiio_adapter_test
        hermes_mpiio)
target_compile_definitions(hermes_mpiio_adapter_test PUBLIC
        HERMES_INTERCEPT=1 HERMES_MPI_TESTS=true)
jarvis_test(mpiio test_hermes_mpiio_basic_sync)
jarvis_test(mpiio test_hermes_mpiio_basic_async)

set(MPIIO_TESTS
  mpiio_adapter_test
  hermes_mpiio_adapter_test
)

foreach(program ${MPIIO_TESTS})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_DIR})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_TEST_DIR})
  target_link_libraries(${program} Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
endforeach()

if(HERMES_INSTALL_TESTS)
  foreach(program ${MPIIO_TESTS})
    install(
      TARGETS
      ${program}
      LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
      ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
      RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
      )
  endforeach()
endif()

add_executable(mpi_parallel parallel.cc)
add_dependencies(mpi_parallel hermes_mpiio)
target_link_libraries(mpi_parallel hermes_mpiio  Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
set_target_properties(mpi_parallel PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")

install(
        TARGETS
        mpi_parallel
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)