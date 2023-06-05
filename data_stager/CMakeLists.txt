#-----------------------------------------------------------------------------
# Build Stage In
#-----------------------------------------------------------------------------
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

add_executable(stage_out ${CMAKE_CURRENT_SOURCE_DIR}/stage_out.cc)
add_dependencies(stage_out hermes_data_stager)
target_link_libraries(stage_out hermes_data_stager)

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION ${HERMES_INSTALL_INCLUDE_DIR}
        FILES_MATCHING PATTERN "*.h")

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_data_stager
        stage_in
        stage_out
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)