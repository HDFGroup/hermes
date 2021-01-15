project(StdioAdapter VERSION 0.1.0)

include_directories(${CMAKE_SOURCE_DIR}/adapter/include)

set(STDIO_ADAPTER_SRC stdio.cpp     metadata_manager.cpp
                                    mapper/balanced_mapper.cpp)
set(STDIO_ADAPTER_PUBLIC_HEADER ${CMAKE_SOURCE_DIR}/adapter/include/hermes/adapter/stdio.h)
set(STDIO_ADAPTER_PRIVATE_HEADER    metadata_manager.h
                                    mapper/mapper_factory.h
                                    mapper/abstract_mapper.h
                                    mapper/balanced_mapper.h common/datastructures.h common/enumerations.h ../singleton.h common/constants.h)

add_library(hermes_stdio SHARED ${STDIO_ADAPTER_PRIVATE_HEADER} ${STDIO_ADAPTER_PUBLIC_HEADER} ${STDIO_ADAPTER_SRC})
add_dependencies(hermes_stdio hermes)
target_link_libraries(hermes_stdio hermes MPI::MPI_CXX)