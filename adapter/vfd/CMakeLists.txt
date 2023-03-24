# CMakeLists files in this project can
# refer to the root source directory of the project as ${HDF5_HERMES_SOURCE_DIR} and
# to the root binary directory of the project as ${HDF5_HERMES_BINARY_DIR}.

project(HDF5_HERMES_VFD)

#------------------------------------------------------------------------------
# Version information
#------------------------------------------------------------------------------
set(HDF5_HERMES_VFD_VERSION_MAJOR ${HERMES_VERSION_MAJOR})
set(HDF5_HERMES_VFD_VERSION_MINOR ${HERMES_VERSION_MINOR})
set(HDF5_HERMES_VFD_VERSION_PATCH ${HERMES_VERSION_PATCH})
set(HDF5_HERMES_VFD_PACKAGE "hdf5_hermes_vfd")
set(HDF5_HERMES_VFD_PACKAGE_NAME "HDF5_HERMES_VFD")
set(HDF5_HERMES_VFD_PACKAGE_VERSION "${HDF5_HERMES_VFD_VERSION_MAJOR}.${HDF5_HERMES_VFD_VERSION_MINOR}.${HDF5_HERMES_VFD_VERSION_PATCH}")
set(HDF5_HERMES_VFD_PACKAGE_VERSION_MAJOR "${HDF5_HERMES_VFD_VERSION_MAJOR}.${HDF5_HERMES_VFD_VERSION_MINOR}")
set(HDF5_HERMES_VFD_PACKAGE_VERSION_MINOR "${HDF5_HERMES_VFD_VERSION_PATCH}")
set(HDF5_HERMES_VFD_PACKAGE_STRING "${HDF5_HERMES_VFD_PACKAGE_NAME} ${HDF5_HERMES_VFD_PACKAGE_VERSION}")
set(HDF5_HERMES_VFD_PACKAGE_TARNAME "${HDF5_HERMES_VFD_PACKAGE}")

# Dynamically loaded VFDs must be shared libraries
set(HDF5_HERMES_VFD_LIBTYPE SHARED)

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if(NOT HDF5_HERMES_VFD_EXPORTED_TARGETS)
  set(HDF5_HERMES_VFD_EXPORTED_TARGETS "${HDF5_HERMES_VFD_PACKAGE}-targets")
endif()

set(HDF5_HERMES_VFD_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/H5FDhermes.h
        ${CMAKE_CURRENT_SOURCE_DIR}/H5FDhermes.cc)

add_library(hdf5_hermes_vfd ${HDF5_HERMES_VFD_SRCS})
target_include_directories(hdf5_hermes_vfd
  SYSTEM PUBLIC ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
)
add_dependencies(hdf5_hermes_vfd
        hermes_posix)
target_link_libraries(hdf5_hermes_vfd
        hermes_posix
        MPI::MPI_CXX
        ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES})

set(HDF5_HERMES_VFD_EXPORTED_LIBS hdf5_hermes_vfd ${HDF5_HERMES_VFD_EXPORTED_LIBS} PARENT_SCOPE)

#-----------------------------------------------------------------------------
# Specify project header files to be installed
#-----------------------------------------------------------------------------
set(HDF5_HERMES_VFD_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/H5FDhermes.h
)

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  FILES
    ${HDF5_HERMES_VFD_HEADERS}
  DESTINATION
    ${HERMES_INSTALL_INCLUDE_DIR}
  COMPONENT
    headers
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  TARGETS
    hdf5_hermes_vfd
  EXPORT
    ${HDF5_HERMES_VFD_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}/${HERMES_VFD_DIR_NAME}
  ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}/${HERMES_VFD_DIR_NAME}
  RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
  EXPORT
    ${HDF5_HERMES_VFD_EXPORTED_TARGETS}
  DESTINATION
    ${HERMES_INSTALL_DATA_DIR}/cmake/hdf5_hermes_vfd
  FILE
    ${HDF5_HERMES_VFD_EXPORTED_TARGETS}.cmake
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project

if(NOT HDF5_HERMES_VFD_EXTERNALLY_CONFIGURED)
  export(
    TARGETS
      ${HDF5_HERMES_VFD_EXPORTED_LIBS}
    FILE
      ${HDF5_HERMES_VFD_EXPORTED_TARGETS}.cmake
  )
endif()

#------------------------------------------------------------------------------
# Set variables for parent scope
#------------------------------------------------------------------------------

# Pkg-config configuration
if(CMAKE_BUILD_TYPE)
  string(TOLOWER ${CMAKE_BUILD_TYPE} lower_cmake_build_type)
endif()

# Hermes VFD package dependencies
foreach(pkg_dep ${HDF5_HERMES_VFD_EXT_PKG_DEPENDENCIES})
  set(HDF5_HERMES_VFD_PKG_DEPENDENCIES ${HDF5_HERMES_VFD_PKG_DEPENDENCIES} ${pkg_dep})
endforeach()
set(HDF5_HERMES_VFD_PKG_DEPENDENCIES ${HDF5_HERMES_VFD_PKG_DEPENDENCIES} PARENT_SCOPE)

# Hermes VFD private library dependencies
foreach(exported_lib ${HDF5_HERMES_VFD_EXPORTED_LIBS})
  if(lower_cmake_build_type MATCHES "debug")
    get_target_property(HDF5_HERMES_VFD_LIBRARY ${exported_lib} DEBUG_OUTPUT_NAME)
  else()
    get_target_property(HDF5_HERMES_VFD_LIBRARY ${exported_lib} RELEASE_OUTPUT_NAME)
  endif()
  set(HDF5_HERMES_VFD_LIBRARIES "${HDF5_HERMES_VFD_LIBRARIES} -l${HDF5_HERMES_VFD_LIBRARY}")
endforeach()
set(HDF5_HERMES_VFD_LIBRARIES ${HDF5_HERMES_VFD_LIBRARIES} PARENT_SCOPE)
# Hermes VFD external library dependencies
# Need to generate -lib if not already passed
set(HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES
  ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES}
  ${HDF5_HERMES_VFD_EXT_PKG_LIB_DEPENDENCIES}
  PARENT_SCOPE
)
foreach(lib_dep ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES})
  # get library name
  get_filename_component(lib_name ${lib_dep} NAME_WE)
  if(lib_name MATCHES "^-l")
    # lib_name found is -lxxx
    set(HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES_LIST ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES_LIST} ${lib_name})
  else()
    # lib_name is /path/to/lib so get library path and name
    get_filename_component(lib_path ${lib_dep} PATH)
    string(REGEX REPLACE "^lib" "" lib_name ${lib_name})
    set(HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES_LIST ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES_LIST} -L${lib_path} -l${lib_name})
  endif()
endforeach()
if(HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES_LIST)
  list(REMOVE_DUPLICATES HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES_LIST)
endif()
foreach(lib_dep ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES_LIST})
  set(HDF5_HERMES_VFD_LIB_DEPENDENCIES "${HDF5_HERMES_VFD_LIB_DEPENDENCIES} ${lib_dep}")
endforeach()
set(HDF5_HERMES_VFD_LIB_DEPENDENCIES ${HDF5_HERMES_VFD_LIB_DEPENDENCIES} PARENT_SCOPE)

# External include dependencies
set(HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES
  ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
  ${HDF5_HERMES_VFD_EXT_PKG_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)
if(HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES)
  list(REMOVE_DUPLICATES HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES)
endif()
foreach(inc_dep ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES})
  set(HDF5_HERMES_VFD_INCLUDE_DEPENDENCIES "${HDF5_HERMES_VFD_INCLUDE_DEPENDENCIES} -I${inc_dep}")
endforeach()
set(HDF5_HERMES_VFD_INCLUDE_DEPENDENCIES ${HDF5_HERMES_VFD_INCLUDE_DEPENDENCIES} PARENT_SCOPE)

set(HDF5_HERMES_VFD_INCLUDES_BUILD_TIME
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)

set(HDF5_HERMES_VFD_INCLUDES_INSTALL_TIME
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
  PARENT_SCOPE
)
