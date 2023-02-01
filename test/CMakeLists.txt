#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
  ${PROJECT_SOURCE_DIR}/gotcha_intercept
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${HERMES_INCLUDES_BUILD_TIME}
)

#------------------------------------------------------------------------------
# API tests
#------------------------------------------------------------------------------
set(API_TESTS
        test_rpc
        test_bucket
        test_vbucket)

foreach(program ${API_TESTS})
  add_executable(${program} ${program}.cc main_mpi.cc)
  target_link_libraries(${program} ${LIBRT} hermes MPI::MPI_CXX
   $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> glog::glog Catch2::Catch2)
  add_test(NAME "Test${program}"
    COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2
    "${CMAKE_BINARY_DIR}/bin/${program}")
  set_tests_properties("Test${program}" PROPERTIES ENVIRONMENT
    LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp)
endforeach()