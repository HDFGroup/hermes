#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

#-----------------------------------------------------------------------------
# Define Sources and tests
#-----------------------------------------------------------------------------
set(hermes_vfd_tests
  hermes_set_fapl
  hermes_driver
  hermes_env
)

foreach(vfd_test ${hermes_vfd_tests})
  add_executable(${vfd_test}
    ${CMAKE_CURRENT_SOURCE_DIR}/${vfd_test}.c
  )
  target_include_directories(${vfd_test}
    PUBLIC  "$<BUILD_INTERFACE:${HDF5_HERMES_VFD_BUILD_INCLUDE_DEPENDENCIES}>"
            $<INSTALL_INTERFACE:${HDF5_HERMES_VFD_INSTALL_INCLUDE_INTERFACE}>
  )
  target_include_directories(${vfd_test}
    SYSTEM PUBLIC ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
  )
  target_link_libraries(${vfd_test}
      ${HDF5_HERMES_VFD_EXPORTED_LIBS}
      ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES}
      ${HDF5_HERMES_VFD_EXT_PKG_DEPENDENCIES}
  )
endforeach()

#-----------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------
add_test(NAME "Testhermes_set_fapl" COMMAND hermes_set_fapl)
set_tests_properties(Testhermes_set_fapl PROPERTIES
  ENVIRONMENT "HERMES_CONF=${CMAKE_CURRENT_SOURCE_DIR}/hermes.conf"
)

add_test(NAME "Testhermes_driver" 
         COMMAND hermes_driver -E env HERMES_CONF=${CMAKE_CURRENT_SOURCE_DIR}/hermes.conf HDF5_PLUGIN_PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

add_test(NAME "Testhermes_env"
         COMMAND hermes_driver -E env HERMES_CONF=${CMAKE_CURRENT_SOURCE_DIR}/hermes.conf HDF5_PLUGIN_PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY} HDF5_DRIVER_CONFIG="true 1024" HDF5_DRIVER=hermes)
