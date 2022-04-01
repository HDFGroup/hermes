macro(EXEC_CHECK CMD)
  execute_process(COMMAND ${CMD} RESULT_VARIABLE CMD_RESULT)
  if(CMD_RESULT)
    message(FATAL_ERROR "Error running ${CMD}")
  endif()
endmacro()

# Run with sec2 (POSIX) VFD
exec_check($<TARGET_FILE:hermes_vfd_test>)

# Run with Hermes VFD
set(ENV{HERMES_CONF} ${CMAKE_CURRENT_SOURCE_DIR}/hermes.conf)
set(ENV{HDF5_PLUGIN_PATH} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(ENV{HDF5_DRIVER_CONFIG} "true 262144")
set(ENV{HDF5_DRIVER} hermes)
set(ENV{LD_PRELOAD} $<TARGET_FILE:hdf5_hermes_vfd>)
exec_check($<TARGET_FILE:hermes_vfd_test>)
unset(ENV{HERMES_CONF})
unset(ENV{HDF5_PLUGIN_PATH})
unset(ENV{HDF5_DRIVER_CONFIG})
unset(ENV{HDF5_DRIVER})
unset(ENV{LD_PRELOAD})

# Use h5diff to make sure the resulting files are identical

# for each test file:
#   construct filenames
#   open file1
#   open file2
exec_check(${HDF5_DIFF_EXECUTABLE} file1 file2)
