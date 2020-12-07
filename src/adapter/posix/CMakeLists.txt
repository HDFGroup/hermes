project(PosixAdapter VERSION 0.1.0)

include_directories(${CMAKE_SOURCE_DIR}/include)

set(POSIX_ADAPTER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/posix.cpp)
set(POSIX_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/include/hermes/adapter/posix.h)
set(POSIX_ADAPTER_PRIVATE_HEADER )

add_library(hermes_posix ${POSIX_ADAPTER_PRIVATE_HEADER} ${POSIX_ADAPTER_PUBLIC_HEADER} ${POSIX_ADAPTER_SRC})
target_link_libraries(hermes_posix hermes MPI::MPI_CXX)