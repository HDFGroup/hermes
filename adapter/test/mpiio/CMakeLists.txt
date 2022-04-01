add_executable(mpiio_adapter_test mpiio_adapter_test.cpp ${ADAPTER_COMMON})
mpi(mpiio_adapter_test 2 "")

add_executable(hermes_mpiio_adapter_test mpiio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_mpiio_adapter_test hermes_mpiio)
add_dependencies(hermes_mpiio_adapter_test hermes_mpiio hermes_daemon)
set_target_properties(hermes_mpiio_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")

mpi_daemon(hermes_mpiio_adapter_test 2 "[synchronicity=async] -d yes" "async" 1)
mpi_daemon(hermes_mpiio_adapter_test 2 "[synchronicity=sync] -d yes" "sync" 1)

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
