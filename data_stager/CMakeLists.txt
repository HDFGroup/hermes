include_directories(${CMAKE_SOURCE_DIR}/adapter)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_RPC_THALLIUM")

add_executable(stage_in ${CMAKE_CURRENT_SOURCE_DIR}/stager.cc)
add_dependencies(stage_in hermes_posix_backend)
target_link_libraries(stage_in hermes_posix_backend)