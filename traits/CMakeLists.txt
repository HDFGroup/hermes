
set(HERMES_IO_CLIENT_DIR ${CMAKE_SOURCE_DIR}/io_client)
set(HERMES_SRC_DIR ${CMAKE_SOURCE_DIR}/src)
set(HERMES_API_DIR ${CMAKE_SOURCE_DIR}/src/api)

include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_API_DIR}
        ${HERMES_IO_CLIENT_DIR})

add_subdirectory(example)
add_subdirectory(prefetcher)

