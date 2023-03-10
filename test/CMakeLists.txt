#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
  ${PROJECT_SOURCE_DIR}/gotcha_intercept
  ${PROJECT_SOURCE_DIR}/io_client
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${HERMES_INCLUDES_BUILD_TIME}
)

#------------------------------------------------------------------------------
# API tests
#------------------------------------------------------------------------------
set(API_TESTS
        test_bucket
        test_partial_put_get
        test_buffer_pool
        test_tag)

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
