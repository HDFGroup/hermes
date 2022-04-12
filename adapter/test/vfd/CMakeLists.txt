set(HERMES_VFD_DIR ${HERMES_ADAPTER_DIR}/vfd)

set(hermes_vfd_tests
  hermes_vfd_test
)

add_executable(hermes_vfd_test ${CMAKE_CURRENT_SOURCE_DIR}/hermes_vfd_test.cc)
target_include_directories(hermes_vfd_test PRIVATE ${HERMES_VFD_DIR})
target_include_directories(hermes_vfd_test PRIVATE ${HERMES_ADAPTER_TEST_DIR})
target_include_directories(hermes_vfd_test
  SYSTEM PRIVATE ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
)

target_link_libraries(hermes_vfd_test
  hermes
  MPI::MPI_CXX
  -lstdc++fs
  ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES}
  ${HDF5_HERMES_VFD_EXT_PKG_DEPENDENCIES}
)

if(HERMES_USE_ADDRESS_SANITIZER)
  execute_process(COMMAND ${CMAKE_C_COMPILER} -print-file-name=libasan.so
    OUTPUT_VARIABLE LIBASAN_PATH
    RESULT_VARIABLE ASAN_PRINT_FILE_NAME_RESULT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  if(ASAN_PRINT_FILE_NAME_RESULT EQUAL 0)
    message(STATUS "Found libasan.so at ${LIBASAN_PATH}")
  else()
    message(FATAL_ERROR
      "Couldn't find the path to libasan.so which is required by the Hermes HDF5 VFD tests. \
       Recompile with HERMES_ENABLE_ADDRESS_SANITIZER=OFF")
  endif()
endif()

add_test(NAME "TestVfd" COMMAND hermes_vfd_test --reporter compact -d yes)
set_property(TEST "TestVfd"
  PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_CURRENT_SOURCE_DIR}/hermes.conf)
# TEMP(chogan):
set_property(TEST "TestVfd" APPEND
  PROPERTY ENVIRONMENT HDF5_PLUGIN_PATH=/home/chogan/local/share/hdf5_plugins)
set_property(TEST "TestVfd" APPEND
  PROPERTY ENVIRONMENT HDF5_DRIVER_CONFIG=true\ 1024)
set_property(TEST "TestVfd" APPEND
  PROPERTY ENVIRONMENT HDF5_DRIVER=hermes)
set_property(TEST "TestVfd" APPEND
  PROPERTY ENVIRONMENT LD_PRELOAD=${LIBASAN_PATH}:$<TARGET_FILE:hdf5_hermes_vfd>)
set_property(TEST "TestVfd" APPEND
  PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)

# IOR tests
if(HERMES_HAVE_IOR)
  set(IOR_TEST_NAME "TestVfdIor")
  add_test(NAME ${IOR_TEST_NAME} COMMAND ${IOR_EXE} -a HDF5 -w -W -r -R)
  set_property(TEST ${IOR_TEST_NAME}
    PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_CURRENT_SOURCE_DIR}/hermes.conf)
  set_property(TEST ${IOR_TEST_NAME} APPEND
    PROPERTY ENVIRONMENT HDF5_PLUGIN_PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
  set_property(TEST ${IOR_TEST_NAME} APPEND
    PROPERTY ENVIRONMENT HDF5_DRIVER_CONFIG=true\ 262144)
  set_property(TEST ${IOR_TEST_NAME} APPEND
    PROPERTY ENVIRONMENT HDF5_DRIVER=hermes)
  set_property(TEST ${IOR_TEST_NAME} APPEND
    PROPERTY ENVIRONMENT LD_PRELOAD=${LIBASAN_PATH}:$<TARGET_FILE:hdf5_hermes_vfd>)
  set_property(TEST ${IOR_TEST_NAME} APPEND
    PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endif()
