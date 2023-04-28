
set(HERMES_IO_CLIENT_DIR ${CMAKE_SOURCE_DIR}/io_client)
set(HERMES_SRC_DIR ${CMAKE_SOURCE_DIR}/src)
set(HERMES_API_DIR ${CMAKE_SOURCE_DIR}/src/api)
set(HERMES_WRAPPER_DIR ${CMAKE_SOURCE_DIR}/wrapper)

include_directories(
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_API_DIR}
        ${HERMES_WRAPPER_DIR}
        ${HERMES_IO_CLIENT_DIR})

if (HERMES_ENABLE_C_BINDINGS OR
        HERMES_ENABLE_JAVA_BINDINGS OR
        HERMES_ENABLE_PYTHON_BINDINGS)
    add_subdirectory(c)
endif()

if (HERMES_ENABLE_JAVA_BINDINGS)
    add_subdirectory(java)
endif()

if (HERMES_ENABLE_PYTHON_BINDINGS)
    add_subdirectory(python)
endif()
