project(StdioAdapter VERSION 0.1.0)

include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

set(STDIO_ADAPTER_SRC stdio.cpp)
set(STDIO_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/adapter/include/hermes/adapter/stdio.h)
set(STDIO_ADAPTER_PRIVATE_HEADER )

add_library(hermes_stdio ${STDIO_ADAPTER_PRIVATE_HEADER} ${STDIO_ADAPTER_PUBLIC_HEADER} ${STDIO_ADAPTER_SRC})
add_dependencies(hermes_stdio hermes)
target_link_libraries(hermes_stdio hermes MPI::MPI_CXX)