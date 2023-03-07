
set(HERMES_IO_CLEINT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_IO_CLIENT_DIR})

add_subdirectory(filesystem)
add_subdirectory(io_client)
add_subdirectory(mpiio)
add_subdirectory(posix)
add_subdirectory(stdio)
