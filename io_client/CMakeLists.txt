
set(HERMES_IO_CLIENT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(HERMES_SRC_DIR ${CMAKE_SOURCE_DIR}/src)

include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_IO_CLIENT_DIR})

add_subdirectory(filesystem)
add_subdirectory(io_client)
add_subdirectory(mpiio)
add_subdirectory(posix)
add_subdirectory(stdio)
