include_directories(
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/adapter
        ${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_RPC_THALLIUM")

add_library(hermes_data_stager
        ${CMAKE_CURRENT_SOURCE_DIR}/stagers/posix_stager.cc)
add_dependencies(hermes_data_stager hermes_posix_io_client)
target_link_libraries(hermes_data_stager hermes_posix_io_client)

add_executable(stage_in ${CMAKE_CURRENT_SOURCE_DIR}/stage_in.cc)
add_dependencies(stage_in hermes_data_stager)
target_link_libraries(stage_in hermes_data_stager)
