# CMakeLists files in this project can
# refer to the root source directory of the project as ${HERMES_SOURCE_DIR} and
# to the root binary directory of the project as ${HERMES_BINARY_DIR}.
cmake_minimum_required (VERSION 3.10)

# Set a consistent MACOSX_RPATH default across all CMake versions.
if(NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_MACOSX_RPATH 0)
endif()

project(HERMES)

include_directories(${PROJECT_BINARY_DIR})

#------------------------------------------------------------------------------
# Version information
#------------------------------------------------------------------------------
set(HERMES_VERSION_MAJOR "0")
set(HERMES_VERSION_MINOR "1")
set(HERMES_VERSION_PATCH "0")
set(HERMES_PACKAGE "hermes")
set(HERMES_PACKAGE_NAME "HERMES")
set(HERMES_PACKAGE_VERSION "${HERMES_VERSION_MAJOR}.${HERMES_VERSION_MINOR}.${HERMES_VERSION_PATCH}")
set(HERMES_PACKAGE_VERSION_MAJOR "${HERMES_VERSION_MAJOR}.${HERMES_VERSION_MINOR}")
set(HERMES_PACKAGE_VERSION_MINOR "${HERMES_VERSION_PATCH}")
set(HERMES_PACKAGE_STRING "${HERMES_PACKAGE_NAME} ${HERMES_PACKAGE_VERSION}")
set(HERMES_PACKAGE_TARNAME "${HERMES_PACKAGE}")

#------------------------------------------------------------------------------
# Setup install and output Directories
#------------------------------------------------------------------------------
if(NOT HERMES_INSTALL_BIN_DIR)
  set(HERMES_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT HERMES_INSTALL_LIB_DIR)
  set(HERMES_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT HERMES_INSTALL_INCLUDE_DIR)
  set(HERMES_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif()
if(NOT HERMES_INSTALL_DATA_DIR)
  set(HERMES_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()

#------------------------------------------------------------------------------
# Setup CMake Environment
#------------------------------------------------------------------------------
if(APPLE)
  # We are doing a unix-style install i.e. everything will be installed in
  # CMAKE_INSTALL_PREFIX/bin and CMAKE_INSTALL_PREFIX/lib etc. as on other unix
  # platforms. We still need to setup CMAKE_INSTALL_NAME_DIR correctly so that
  # the binaries point to appropriate location for the libraries.

  # 1. Make CMAKE_INSTALL_PREFIX publicly accessible, if it was hidden in
  #    previous pass
  get_property(is_internal CACHE CMAKE_INSTALL_PREFIX PROPERTY TYPE)
  if(is_internal STREQUAL "INTERNAL")
    set(CMAKE_INSTALL_PREFIX ${CACHED_CMAKE_INSTALL_PREFIX} CACHE PATH "Install prefix" FORCE)
  else()
    set(CMAKE_INSTALL_PREFIX ${CACHED_CMAKE_INSTALL_PREFIX} CACHE PATH "Install prefix")
  endif()
  unset(MACOSX_APP_INSTALL_PREFIX CACHE)

  set(CMAKE_INSTALL_NAME_DIR "@rpath")
  mark_as_advanced(
    CMAKE_OSX_ARCHITECTURES
    CMAKE_OSX_DEPLOYMENT_TARGET
    CMAKE_OSX_SYSROOT
    )
endif()

#------------------------------------------------------------------------------
if(NOT HERMES_EXTERNALLY_CONFIGURED)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Executables."
  )
  set(EXECUTABLE_OUTPUT_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Libraries"
  )
  set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
    ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all static libraries."
  )
endif()

set(HERMES_CMAKE_DIR "${HERMES_SOURCE_DIR}/CMake")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${HERMES_CMAKE_DIR})

#------------------------------------------------------------------------------
# Disallow in-source build
#------------------------------------------------------------------------------
if("${HERMES_SOURCE_DIR}" STREQUAL "${HERMES_BINARY_DIR}")
  message(FATAL_ERROR
    "HERMES requires an out of source Build. "
    "Please create a separate binary directory and run CMake there.")
endif()

#------------------------------------------------------------------------------
# Set a default build type if none was specified
#------------------------------------------------------------------------------
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

if(NOT "${CMAKE_CXX_STANDARD}")
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif()

if(NOT CMAKE_CXX_FLAGS)
#TODO: add -Werror
 set (CMAKE_CXX_FLAGS "-Wall -Wextra ${CMAKE_CXX_FLAGS}")
 message (STATUS "Warnings Configuration: default: ${CMAKE_CXX_FLAGS}")
endif()
#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if(NOT HERMES_EXPORTED_TARGETS)
  set(HERMES_EXPORTED_TARGETS "hermes-targets")
endif()

#------------------------------------------------------------------------------
# Choose static or shared libraries.
#------------------------------------------------------------------------------
option(BUILD_SHARED_LIBS "Build with shared libraries." OFF)

#-------------------------------------------------------------------------------
function(hermes_set_lib_options libtarget libname libtype)
  if(${libtype} MATCHES "SHARED")
    if(WIN32 AND NOT MINGW)
      set(LIB_RELEASE_NAME "${libname}")
      set(LIB_DEBUG_NAME "${libname}_D")
      set(LIB_VERSION ${HERMES_PACKAGE_VERSION_MAJOR})
    else()
      set(LIB_RELEASE_NAME "${libname}")
      set(LIB_DEBUG_NAME "${libname}_debug")
      set(LIB_VERSION ${HERMES_PACKAGE_VERSION})
    endif()
  else()
    if(WIN32 AND NOT MINGW)
      set(LIB_RELEASE_NAME "lib${libname}")
      set(LIB_DEBUG_NAME "lib${libname}_D")
    else()
      # if the generator supports configuration types or if the CMAKE_BUILD_TYPE has a value
      if(CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE)
        set(LIB_RELEASE_NAME "${libname}")
        set(LIB_DEBUG_NAME "${libname}_debug")
      else()
        set(LIB_RELEASE_NAME "lib${libname}")
        set(LIB_DEBUG_NAME "lib${libname}_debug")
      endif()
    endif()
  endif()

  set_target_properties(${libtarget}
    PROPERTIES
    DEBUG_OUTPUT_NAME          ${LIB_DEBUG_NAME}
    RELEASE_OUTPUT_NAME        ${LIB_RELEASE_NAME}
    MINSIZEREL_OUTPUT_NAME     ${LIB_RELEASE_NAME}
    RELWITHDEBINFO_OUTPUT_NAME ${LIB_RELEASE_NAME}
    VERSION                    ${LIB_VERSION}
    SOVERSION                  ${LIB_VERSION}
  )

  #----- Use MSVC Naming conventions for Shared Libraries
  if(MINGW AND ${libtype} MATCHES "SHARED")
    set_target_properties(${libtarget}
      PROPERTIES
      IMPORT_SUFFIX ".lib"
      IMPORT_PREFIX ""
      PREFIX ""
    )
  endif()
endfunction()

#-----------------------------------------------------------------------------
# Source
#-----------------------------------------------------------------------------
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the build directory
#-----------------------------------------------------------------------------
configure_file(
 ${HERMES_SOURCE_DIR}/CMake/hermes-config.cmake.build.in
 ${HERMES_BINARY_DIR}/hermes-config.cmake @ONLY
)

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
 ${HERMES_SOURCE_DIR}/CMake/hermes-config.cmake.install.in
 ${HERMES_BINARY_DIR}/CMakeFiles/hermes-config.cmake @ONLY
)

install(
  FILES
    ${HERMES_BINARY_DIR}/CMakeFiles/hermes-config.cmake
  DESTINATION
    ${HERMES_INSTALL_DATA_DIR}/cmake/hermes
)

#-----------------------------------------------------------------------------
# Configure the pdc-config-version .cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
  ${HERMES_SOURCE_DIR}/CMake/hermes-config-version.cmake.in
  ${HERMES_BINARY_DIR}/CMakeFiles/hermes-config-version.cmake @ONLY
)

install(
  FILES
    ${HERMES_BINARY_DIR}/CMakeFiles/hermes-config-version.cmake
  DESTINATION
    ${HERMES_INSTALL_DATA_DIR}/cmake/hermes
)
