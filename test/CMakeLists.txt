#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${HERMES_INCLUDES_BUILD_TIME}
)


#------------------------------------------------------------------------------
# API tests
#------------------------------------------------------------------------------
set(API_TESTS bucket_test)

find_package(ZLIB)
if(ZLIB_FOUND)
  message(STATUS "found zlib")
endif()

foreach(program ${API_TESTS})
  add_executable(${program} ${program}.cpp)
  target_link_libraries(${program} hermes ZLIB::ZLIB)
endforeach()

#------------------------------------------------------------------------------
# BufferPool tests
#------------------------------------------------------------------------------

add_executable(bp buffer_pool_test.cc)
target_link_libraries(bp ${LIBRT} hermes MPI::MPI_CXX)

add_executable(bp_test buffer_pool_client_test.cc)
target_link_libraries(bp_test hermes thallium MPI::MPI_CXX)

add_executable(end_to_end_test end_to_end_test.cc)
target_link_libraries(end_to_end_test hermes ${LIBRT} MPI::MPI_CXX)

#------------------------------------------------------------------------------
# Config Parser tests
#------------------------------------------------------------------------------

add_executable(config_parser_test config_parser_test.cc)
target_link_libraries(config_parser_test hermes)

add_test(NAME TestConfigParser
  COMMAND config_parser_test ${CMAKE_CURRENT_SOURCE_DIR}/data/hermes.conf)
