project(PosixAdapter VERSION 0.1.0)

include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

set(POSIX_ADAPTER_SRC posix.cpp)
set(POSIX_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/adapter/include/hermes/adapter/posix.h)
set(POSIX_ADAPTER_PRIVATE_HEADER )

add_library(hermes_posix SHARED ${POSIX_ADAPTER_PRIVATE_HEADER} ${POSIX_ADAPTER_PUBLIC_HEADER} ${POSIX_ADAPTER_SRC})
add_dependencies(hermes_posix hermes)
target_link_libraries(hermes_posix hermes MPI::MPI_CXX)