set(HERMES_VFD_DIR ${HERMES_ADAPTER_DIR}/vfd)

set(hermes_vfd_tests
  hermes_set_fapl
  hermes_driver
  hermes_env
)

foreach(vfd_test ${hermes_vfd_tests})
  add_executable(${vfd_test} ${CMAKE_CURRENT_SOURCE_DIR}/${vfd_test}.c)
  target_include_directories(${vfd_test} PRIVATE ${HERMES_VFD_DIR})
  target_include_directories(${vfd_test}
    SYSTEM PUBLIC ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
  )
  target_link_libraries(${vfd_test}
    MPI::MPI_C
    ${HDF5_HERMES_VFD_EXPORTED_LIBS}
    ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES}
    ${HDF5_HERMES_VFD_EXT_PKG_DEPENDENCIES}
    )
  add_test(NAME "Test${vfd_test}" COMMAND ${vfd_test})
  set_property(TEST "Test${vfd_test}"
    PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_CURRENT_SOURCE_DIR}/hermes.conf)
  set_property(TEST "Test${vfd_test}" APPEND
    PROPERTY ENVIRONMENT HDF5_PLUGIN_PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
  set_property(TEST "Test${vfd_test}" APPEND
    PROPERTY ENVIRONMENT HDF5_DRIVER_CONFIG="true 1024")
  set_property(TEST "Test${vfd_test}" APPEND
    PROPERTY ENVIRONMENT HDF5_DRIVER=hermes)
endforeach()
