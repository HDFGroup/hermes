
set(HERMES_IO_CLIENT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(HERMES_SRC_DIR ${CMAKE_SOURCE_DIR}/src)

include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_IO_CLIENT_DIR})

add_subdirectory(filesystem)
add_subdirectory(io_client)

if(HERMES_ENABLE_POSIX_ADAPTER)
    add_subdirectory(posix)
endif()
if(HERMES_ENABLE_STDIO_ADAPTER)
    add_subdirectory(stdio)
endif()
if(HERMES_ENABLE_MPIIO_ADAPTER)
    add_subdirectory(mpiio)
endif()
