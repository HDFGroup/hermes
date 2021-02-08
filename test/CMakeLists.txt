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
set(API_TESTS dpe_optimization_test dpe_random_test
    dpe_roundrobin_test end_to_end_test)

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
  target_link_libraries(mpi_test hermes MPI::MPI_CXX)
endif()

#------------------------------------------------------------------------------
# BufferPool tests
#------------------------------------------------------------------------------

add_executable(bp buffer_pool_test.cc)
target_link_libraries(bp ${LIBRT} hermes MPI::MPI_CXX
    $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
target_compile_definitions(bp
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
add_test(NAME "TestBufferPool" COMMAND "${CMAKE_BINARY_DIR}/bin/bp" "-b" "-s")
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
target_link_libraries(config_parser_test hermes
  $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)

add_test(NAME TestConfigParser
  COMMAND config_parser_test ${CMAKE_CURRENT_SOURCE_DIR}/data/hermes.conf)
target_compile_definitions(config_parser_test
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)

#------------------------------------------------------------------------------
# Memory Management tests
#------------------------------------------------------------------------------

add_executable(mem memory_test.cc)
target_link_libraries(mem hermes $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
add_test(NAME TestMemoryManagement COMMAND mem)
target_compile_definitions(mem
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)

add_executable(stb_map stb_map_test.cc)
target_link_libraries(stb_map hermes ${LIBRT}
  $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> MPI::MPI_CXX)
target_compile_definitions(stb_map
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
add_test(NAME TestSTBMapWithHeap COMMAND stb_map)

#------------------------------------------------------------------------------
# Metadata Manager tests
#------------------------------------------------------------------------------

add_executable(mdm mdm_test.cc)
target_link_libraries(mdm hermes ${LIBRT}
  $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> MPI::MPI_CXX)
target_compile_definitions(mdm
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
add_test(NAME TestMDM COMMAND mdm)
set_tests_properties("TestMDM" PROPERTIES ENVIRONMENT
  LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp)

#------------------------------------------------------------------------------
# Trait tests
#------------------------------------------------------------------------------

add_executable(trait trait_test.cc)
target_link_libraries(trait ${LIBRT} hermes MPI::MPI_CXX
$<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> Catch2::Catch2 -lstdc++fs)
target_compile_definitions(trait
PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
add_test(NAME "TestTrait" COMMAND "${CMAKE_BINARY_DIR}/bin/trait" "-s" "--reporter" "compact"
        "--config" "${CMAKE_CURRENT_SOURCE_DIR}/data/bucket_test.conf")
set_tests_properties("TestTrait" PROPERTIES ENVIRONMENT
LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp)

#------------------------------------------------------------------------------
# Custom Targets
#------------------------------------------------------------------------------

add_custom_target(ares
  COMMAND LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp
  ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 -ppn 2
  "${CMAKE_BINARY_DIR}/bin/end_to_end_test"
  "${CMAKE_CURRENT_SOURCE_DIR}/data/ares.conf"
)
if(HERMES_INSTALL_TESTS)
  set(TEST_EXECS dpe_optimization_test dpe_random_test
          dpe_roundrobin_test end_to_end_test
          bucket_test stdio_test bp bp_test config_parser_test stb_map mdm trait)
  foreach(program ${TEST_EXECS})
    install(
            TARGETS
            ${program}
            EXPORT
            ${HERMES_EXPORTED_TARGETS}
            LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
            ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
            RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
    )
  endforeach()
endif()
