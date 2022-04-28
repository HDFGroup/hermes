# Set hermes to preload if adapter is linked.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHERMES_PRELOAD -DHERMES_RPC_THALLIUM")

set(HERMES_ADAPTER_DIR ${CMAKE_SOURCE_DIR}/adapter)

if(HERMES_ENABLE_STDIO_ADAPTER)
  add_subdirectory(stdio)
endif()

if(HERMES_ENABLE_POSIX_ADAPTER)
  add_subdirectory(posix)
endif()

if(HERMES_ENABLE_MPIIO_ADAPTER)
  add_subdirectory(mpiio)
endif()

if(HERMES_ENABLE_PUBSUB_ADAPTER)
  add_subdirectory(pubsub)
endif()

if(HERMES_ENABLE_VFD)
  if(HERMES_ENABLE_WRAPPER)
    # The Hermes VFD cannot be installed into the same directory as libhermes.so
    # because of the way dynamic plugin loading works in HDF5. When searching
    # for a VFD, HDF5 will dlopen and dlclose every shared library in
    # HDF5_PLUGIN_PATH looking for a valid VFD. When HDF5 goes through this
    # process with libhermes.so it resets some statically initialized variables.
    set(HERMES_VFD_DIR_NAME hermes_vfd)
    set(HERMES_VFD_LIBRARY_DIR ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/${HERMES_VFD_DIR_NAME})
    add_subdirectory(vfd)
  else()
    message(FATAL_ERROR "The Hermes VFD requires HERMES_ENABLE_WRAPPER=ON")
  endif()
endif()

if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()
