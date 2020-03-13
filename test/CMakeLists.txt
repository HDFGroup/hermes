#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
  ${CMAKE_CURRENT_SOURCE_DIR}
  
  ${HERMES_INCLUDES_BUILD_TIME}
)

set(PROGRAMS bucket_test)

find_package(ZLIB)
if(ZLIB_FOUND)
  message(STATUS "found zlib")
endif()

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.cpp)
  target_link_libraries(${program} hermes ZLIB::ZLIB)
endforeach()
