# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

# include directory for absolute references to files and headers.
include_directories(${CMAKE_SOURCE_DIR}/adapter/include
        ${CMAKE_SOURCE_DIR}/adapter/src)

# add stdio adapter
add_subdirectory(src/hermes/adapter/stdio)

# add posix adapter
add_subdirectory(src/hermes/adapter/posix)

# add mpiio adapter
add_subdirectory(src/hermes/adapter/mpiio)

# add posix adapter
add_subdirectory(src/hermes/adapter/pubsub)