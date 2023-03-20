#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/src/api
  ${PROJECT_SOURCE_DIR}/gotcha_intercept
  ${PROJECT_SOURCE_DIR}/io_client
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${HERMES_INCLUDES_BUILD_TIME}
)

#------------------------------------------------------------------------------
# Single-node API tests
#------------------------------------------------------------------------------
set(API_TESTS
        test_bucket
        test_partial_put_get
        test_buffer_pool
        test_trait
        test_tag)

find_program(BASH_PROGRAM bash)

add_executable(test_config test_config.cc main.cc)
add_dependencies(test_config hermes)
target_link_libraries(test_config ${LIBRT}
        hermes
        MPI::MPI_CXX
        $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>
        glog::glog
        Catch2::Catch2)
add_test(Testtest_config COMMAND ${CMAKE_BINARY_DIR}/test_config)

foreach(program ${API_TESTS})
  add_executable(${program} ${program}.cc main_mpi.cc)
  add_dependencies(${program} hermes)
  target_link_libraries(${program} ${LIBRT}
          hermes
          MPI::MPI_CXX
          $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>
          glog::glog
          Catch2::Catch2)
  set(script ${CMAKE_SOURCE_DIR}/test/mpi.sh)
  add_test(NAME "Test${program}"
    COMMAND ${BASH_PROGRAM} ${script}
          ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
          ${MPIEXEC_EXECUTABLE}
          ${MPIEXEC_NUMPROC_FLAG}
          2
          ${program}
          "")
endforeach()

#------------------------------------------------------------------------------
# Multi-node API tests
#------------------------------------------------------------------------------
set(API_TESTS
        test_multinode_put_get)

find_program(BASH_PROGRAM bash)

foreach(program ${API_TESTS})
  add_executable(${program} ${program}.cc main_mpi.cc)
  add_dependencies(${program} hermes)
  target_link_libraries(${program} ${LIBRT}
          hermes
          MPI::MPI_CXX
          $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>
          glog::glog
          Catch2::Catch2)
  set(script ${CMAKE_SOURCE_DIR}/test/mpi.sh)
  add_test(NAME "Test${program}"
          COMMAND ${BASH_PROGRAM} ${script}
          ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
          ${MPIEXEC_EXECUTABLE}
          ${MPIEXEC_NUMPROC_FLAG}
          2
          ${program}
          "")
endforeach()