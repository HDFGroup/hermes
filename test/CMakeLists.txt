#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
  ${PROJECT_SOURCE_DIR}/gotcha_intercept
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${HERMES_INCLUDES_BUILD_TIME}
)

#------------------------------------------------------------------------------
# API tests
#------------------------------------------------------------------------------
set(API_TESTS dpe_optimization_test dpe_random_test end_to_end_test)

foreach(program ${API_TESTS})
  add_executable(${program} ${program}.cc)
  target_link_libraries(${program} ${LIBRT} hermes MPI::MPI_CXX
   $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
  target_compile_definitions(${program}
    PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
  add_test(NAME "Test${program}"
    COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2
    "${CMAKE_BINARY_DIR}/bin/${program}")
  set_tests_properties("Test${program}" PROPERTIES ENVIRONMENT
    LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp)
endforeach()

#------------------------------------------------------------------------------
# Bucket test
#------------------------------------------------------------------------------
find_package(ZLIB)
if(ZLIB_FOUND)
  message(STATUS "found zlib")
endif()

add_executable(bucket_test bucket_test.cc)
target_link_libraries(bucket_test ${LIBRT} hermes MPI::MPI_CXX ZLIB::ZLIB
  $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
target_compile_definitions(bucket_test
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
add_test(NAME "Testbucket_test"
  COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2
  "${CMAKE_BINARY_DIR}/bin/bucket_test"
  "${CMAKE_CURRENT_SOURCE_DIR}/data/bucket_test.conf")
set_tests_properties("Testbucket_test" PROPERTIES ENVIRONMENT
  LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp)

#------------------------------------------------------------------------------
# POSIX GOTCHA test
#------------------------------------------------------------------------------
if(HERMES_HAVE_GOTCHA)
  add_executable(stdio_test stdio_test.c)
  target_link_libraries(stdio_test hermes)
  add_executable(mpi_test mpi_test.c)
  target_link_libraries(mpi_test hermes)
endif()

#------------------------------------------------------------------------------
# BufferPool tests
#------------------------------------------------------------------------------

add_executable(bp buffer_pool_test.cc)
target_link_libraries(bp ${LIBRT} hermes MPI::MPI_CXX
    $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
target_compile_definitions(bp
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
add_test(NAME "TestBufferPool" COMMAND "${CMAKE_BINARY_DIR}/bin/bp" "-b")
set_tests_properties("TestBufferPool" PROPERTIES ENVIRONMENT
  LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp)

add_executable(bp_test buffer_pool_client_test.cc)
target_link_libraries(bp_test hermes MPI::MPI_CXX
     $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
target_compile_definitions(bp_test
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)

#------------------------------------------------------------------------------
# Config Parser tests
#------------------------------------------------------------------------------

add_executable(config_parser_test config_parser_test.cc)
target_link_libraries(config_parser_test hermes)

add_test(NAME TestConfigParser
  COMMAND config_parser_test ${CMAKE_CURRENT_SOURCE_DIR}/data/hermes.conf)

#------------------------------------------------------------------------------
# Memory Management tests
#------------------------------------------------------------------------------

add_executable(mem memory_test.cc)
target_link_libraries(mem hermes)
add_test(NAME TestMemoryManagement COMMAND mem)

add_executable(stb_map stb_map_test.cc)
target_link_libraries(stb_map hermes ${LIBRT} MPI::MPI_CXX)
add_test(NAME TestSTBMapWithHeap COMMAND stb_map)

#------------------------------------------------------------------------------
# Custom Targets
#------------------------------------------------------------------------------

add_custom_target(ares
  COMMAND LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp
  ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 -ppn 2
  "${CMAKE_BINARY_DIR}/bin/end_to_end_test"
  "${CMAKE_CURRENT_SOURCE_DIR}/data/ares.conf"
)
