# CMakeLists files in this project can
# refer to the root source directory of the project as ${HERMES_SOURCE_DIR} and
# to the root binary directory of the project as ${HERMES_BINARY_DIR}.
cmake_minimum_required (VERSION 3.10)

if(CMAKE_SIZEOF_VOID_P LESS 8)
  message(FATAL_ERROR "Hermes currently requires a 64-bit processor")
endif()

# Set a consistent MACOSX_RPATH default across all CMake versions.
if(NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_MACOSX_RPATH 0)
endif()

project(HERMES)
set(IS_HERMES_MAIN ON)

#------------------------------------------------------------------------------
# Compiler optimization
#------------------------------------------------------------------------------
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
  # Ensure that debug logging is enabled
  # This will keep logs which have beneath priority 10
  add_compile_definitions(HERMES_LOG_VERBOSITY=10)
  message("This is NOT a release build: ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS}")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  # Ensure that debug logging is disabled
  # This will keep only VLOGs which have priroity 0
  add_compile_definitions(HERMES_LOG_VERBOSITY=1)
  message("This IS a release build: ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS}")
endif()

#------------------------------------------------------------------------------
# Version information
#------------------------------------------------------------------------------
set(HERMES_VERSION_MAJOR "1")
set(HERMES_VERSION_MINOR "0")
set(HERMES_VERSION_PATCH "0")
set(HERMES_PACKAGE "hermes")
set(HERMES_PACKAGE_NAME "HERMES")
set(HERMES_PACKAGE_VERSION "${HERMES_VERSION_MAJOR}.${HERMES_VERSION_MINOR}.${HERMES_VERSION_PATCH}")
set(HERMES_PACKAGE_VERSION_MAJOR "${HERMES_VERSION_MAJOR}.${HERMES_VERSION_MINOR}")
set(HERMES_PACKAGE_VERSION_MINOR "${HERMES_VERSION_PATCH}")
set(HERMES_PACKAGE_STRING "${HERMES_PACKAGE_NAME} ${HERMES_PACKAGE_VERSION}")
set(HERMES_PACKAGE_TARNAME "${HERMES_PACKAGE}")

#set( CMAKE_VERBOSE_MAKEFILE on )

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
# Export compile commands for autocode generation with CLANG
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
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

if(NOT CMAKE_INSTALL_RPATH)
  set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
  set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
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
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${ORTOOLS_DIR} ${ORTOOLS_ROOT})

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

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if(NOT HERMES_EXPORTED_TARGETS)
  set(HERMES_EXPORTED_TARGETS "hermes-targets")
endif()

#------------------------------------------------------------------------------
# Build options
#------------------------------------------------------------------------------
option(BUILD_SHARED_LIBS "Build with shared libraries." ON)
option(HERMES_INTERCEPT_IO "Use GOTCHA as IO interception tool" OFF)
option(HERMES_COMMUNICATION_MPI "Use MPI as the communication layer." ON)
option(HERMES_ENABLE_DOXYGEN "Enable hermes documentation." OFF)
option(HERMES_BUILD_BUFFER_POOL_VISUALIZER "Build the BufferPool visualizer" OFF)
option(HERMES_USE_ADDRESS_SANITIZER "Enable -fsanitize=address in Debug builds" OFF)
option(HERMES_USE_THREAD_SANITIZER "Enable -fsanitize=thread in Debug builds" OFF)
option(HERMES_RPC_THALLIUM "Use Thallium as the RPC library." ON)
option(HERMES_BUILD_BENCHMARKS "Build the Hermes benchmark suite." ON)
option(HERMES_ENABLE_COVERAGE "Enable code coverage." OFF)
option(HERMES_ENABLE_POSIX_ADAPTER "Build the Hermes POSIX adapter." ON)
option(HERMES_ENABLE_STDIO_ADAPTER "Build the Hermes stdio adapter." ON)
option(HERMES_ENABLE_MPIIO_ADAPTER "Build the Hermes MPI-IO adapter." ON)
option(HERMES_ENABLE_PUBSUB_ADAPTER "Build the Hermes pub/sub adapter." ON)
option(HERMES_ENABLE_VFD "Build the Hermes HDF5 Virtual File Driver" OFF)
option(HERMES_ENABLE_GFLAGS "Enable GFLAGS support." ON)
option(HERMES_BUILD_ADAPTER_TESTS "Enable installation of tests." ON)
option(HERMES_INSTALL_TESTS "Enable installation of tests." OFF)
option(HERMES_ONLY_RPC "Avoids the shared-memory case in RPC for testing" OFF)
option(HERMES_ENABLE_PROFILING "Enables profiling of certain functions" OFF)
option(HERMES_DISABLE_INFO_LOGS "Disables GLOG info" OFF)

option(HERMES_PTHREADS_ENABLED "Support spawning pthreads" ON)
option(HERMES_DEBUG_LOCK "Used for debugging locks" OFF)

# Compile definitions
if (HERMES_PTHREADS_ENABLED)
  add_compile_definitions(HERMES_PTHREADS_ENABLED)
endif()
if (HERMES_RPC_THALLIUM)
  add_compile_definitions(HERMES_RPC_THALLIUM)
endif()
if (HERMES_DEBUG_LOCK)
  message("Lock debugging enabled")
  add_compile_definitions(HERMES_DEBUG_LOCK)
endif()

# Disable informatoinal logging
if(HERMES_DISABLE_INFO_LOGS)
  add_compile_options(-DNDEBUG)
endif()

# Calculate code coverage with debug mode
if(HERMES_ENABLE_COVERAGE)
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Use code coverage with debug mode")
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "" FORCE)
  endif()
endif()

if(BUILD_SHARED_LIBS)
  set(HERMES_BUILD_SHARED_LIBS 1)
  set(HERMES_LIBTYPE SHARED)
else()
  set(HERMES_BUILD_SHARED_LIBS 0)
  set(HERMES_LIBTYPE STATIC)
endif()

# TODO(chogan): Expose this once we have more than one communication layer
mark_as_advanced(HERMES_COMMUNICATION_MPI)
# TODO(chogan): Expose this once we support more than one RPC layer
mark_as_advanced(HERMES_RPC_THALLIUM)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  if (NOT "${CMAKE_CXX_FLAGS_DEBUG}" MATCHES ".*-g3.*")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3"
        CACHE STRING "" FORCE)
  endif()

  if (HERMES_USE_ADDRESS_SANITIZER)
    if(NOT "${CMAKE_CXX_FLAGS_DEBUG}" MATCHES ".*-fsanitize=address.*")
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address"
        CACHE STRING "" FORCE)
    endif()
  else()
    string(REPLACE
      "-fsanitize=address" "" FLAGS_NO_SANITIZE "${CMAKE_CXX_FLAGS_DEBUG}")
    set(CMAKE_CXX_FLAGS_DEBUG "${FLAGS_NO_SANITIZE}" CACHE STRING "" FORCE)
  endif()

  if (HERMES_USE_THREAD_SANITIZER AND HERMES_USE_ADDRESS_SANITIZER)
    message(FATAL_ERROR "Cannont use -fsanitize=address and -fsanitize=thread "
                        "at the same time")
  else()
    if (HERMES_USE_THREAD_SANITIZER)
      if(NOT "${CMAKE_CXX_FLAGS_DEBUG}" MATCHES ".*-fsanitize=thread.*")
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread"
          CACHE STRING "" FORCE)
      endif()
    else()
      string(REPLACE
        "-fsanitize=thread" "" FLAGS_NO_SANITIZE "${CMAKE_CXX_FLAGS_DEBUG}")
      set(CMAKE_CXX_FLAGS_DEBUG "${FLAGS_NO_SANITIZE}" CACHE STRING "" FORCE)
    endif()
  endif()
endif()

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
# Dependencies common to all subdirectories
#-----------------------------------------------------------------------------
# thallium
if(HERMES_RPC_THALLIUM)
  find_package(thallium CONFIG REQUIRED)
  if(thallium_FOUND)
    message(STATUS "found thallium at ${thallium_DIR}")
  endif()
endif()

#YAML-CPP
find_package(yaml-cpp REQUIRED)
if(yaml-cpp_FOUND)
  message(STATUS "found yaml-cpp at ${yaml-cpp_DIR}")
endif()

# GFLAGS
if(HERMES_ENABLE_GFLAGS)
  find_package(gflags REQUIRED)
  if(gflags_FOUND)
    message(STATUS "found GFLAGS at ${gflags_DIR}")
    find_path(GFLAG_INCLUDE_DIRS NAME gflags PATH_SUFFIXES include/)
    include_directories(${GFLAG_INCLUDE_DIRS})
  endif()
endif()

# GOTCHA
if(HERMES_INTERCEPT_IO)
  find_package(gotcha REQUIRED)
  if(gotcha_FOUND)
    set(HERMES_HAVE_GOTCHA 1)
    include_directories(${gotcha_INCLUDE_DIRS})
    set(HERMES_EXT_INCLUDE_DEPENDENCIES ${gotcha_INCLUDE_DIRS}
      ${HERMES_EXT_INCLUDE_DEPENDENCIES}
    )
    set(HERMES_EXT_LIB_DEPENDENCIES gotcha ${HERMES_EXT_LIB_DEPENDENCIES})
    option(HERMES_BUILD_POSIX_IO "Build POSIX I/O Module" ON)
    option(HERMES_BUILD_MPI_IO "Build MPI I/O Module" ON)
    option(HERMES_BUILD_STDIO "Build standard I/O Module" ON)
  endif()
endif()

#if(HERMES_COMMUNICATION_MPI)
find_package(MPI REQUIRED COMPONENTS C CXX)
message(STATUS "found mpi.h at ${MPI_CXX_INCLUDE_DIRS}")
#endif()

# librt
if(NOT APPLE)
  find_library(LIBRT rt)
  if(NOT LIBRT)
    message(FATAL_ERROR "librt is required for POSIX shared memory")
  endif()
endif()

# sdl2
if(HERMES_BUILD_BUFFER_POOL_VISUALIZER)
  find_package(SDL2 CONFIG REQUIRED)
  if(SDL2_FOUND)
    message(STATUS "found SDL2 at ${SDL2_DIR}")
  endif()
endif()

# HDF5
if(HERMES_ENABLE_VFD)
  set(HERMES_REQUIRED_HDF5_VERSION 1.13.0)
  set(HERMES_REQUIRED_HDF5_COMPONENTS C)
  find_package(HDF5 ${HERMES_REQUIRED_HDF5_VERSION} CONFIG NAMES hdf5
    COMPONENTS ${HERMES_REQUIRED_HDF5_COMPONENTS} shared)
  if(HDF5_FOUND)
    message(STATUS "found HDF5 ${HDF5_VERSION} at ${HDF5_INCLUDE_DIR}")
    set(HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES
      ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
      ${HDF5_INCLUDE_DIR})
    set(HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES
      ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES}
      ${HDF5_C_SHARED_LIBRARY})
  else()
    # Allow for HDF5 autotools builds
    find_package(HDF5 ${HERMES_REQUIRED_HDF5_VERSION} MODULE REQUIRED
      COMPONENTS ${HERMES_REQUIRED_HDF5_COMPONENTS})
    if(HDF5_FOUND)
      set(HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES
        ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
        ${HDF5_INCLUDE_DIRS})
      set(HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES
        ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES}
        ${HDF5_LIBRARIES})
    else()
      message(FATAL_ERROR "Could not find HDF5, please set HDF5_DIR (1.13.0) or HDF5_ROOT (1.13.1).")
    endif()
  endif()
endif()

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
  set(COVERAGE_FLAGS "-fprofile-arcs -ftest-coverage" CACHE STRING
    "Flags to the coverage program to perform coverage inspection"
  )
  mark_as_advanced(COVERAGE_FLAGS)

  macro(set_coverage_flags target)
    set_target_properties(${target}
      PROPERTIES
        COMPILE_FLAGS ${COVERAGE_FLAGS}
        LINK_FLAGS ${COVERAGE_FLAGS}
    )
  endmacro()
endif()

#-----------------------------------------------------------------------------
# Source
#-----------------------------------------------------------------------------

set(HERMES_EXPORTED_LIBS "")

include_directories(${CMAKE_SOURCE_DIR}/hermes_shm/include)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src)
add_subdirectory(${CMAKE_SOURCE_DIR}/hermes_shm)
add_subdirectory(${CMAKE_SOURCE_DIR}/io_client)
add_subdirectory(${CMAKE_SOURCE_DIR}/wrapper)

add_custom_target(rpc COMMAND bash
        ${CMAKE_SOURCE_DIR}/code_generators/code_generators/rpc/rpcgen.sh
        "${CMAKE_SOURCE_DIR}"
        "${CMAKE_BINARY_DIR}")
# add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/data_stager)

#-----------------------------------------------------------------------------
# Testing
#-----------------------------------------------------------------------------
include(CTest)
if(CMAKE_PROJECT_NAME STREQUAL HERMES AND BUILD_TESTING)
  find_package(Catch2 3.0.1 REQUIRED)  
  find_program(IOR_EXE ior)
  if(IOR_EXE)
    message(STATUS "Found ior at ${IOR_EXE}")
    set(HERMES_HAVE_IOR "YES")
  else()
    message(WARNING "Couldn't find the 'ior' executable. Some tests will be skipped.")
    set(HERMES_HAVE_IOR "NO")
  endif()
  enable_testing()
  add_subdirectory(test)
  # add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/data_stager/test)
endif()

##### ADAPTERS
if(HERMES_ENABLE_STDIO_ADAPTER OR HERMES_ENABLE_POSIX_ADAPTER OR
    HERMES_ENABLE_MPIIO_ADAPTER OR HERMES_ENABLE_PUBSUB_ADAPTER OR
    HERMES_ENABLE_VFD)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/adapter)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/traits)
endif()

#-----------------------------------------------------------------------------
# Documentation
#-----------------------------------------------------------------------------

if(HERMES_ENABLE_DOXYGEN)
  include(UseDoxygenDoc)

  add_doxygen_doc(
    BUILD_DIR
      ${CMAKE_CURRENT_BINARY_DIR}/_build
    DOXY_FILE
      ${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in
    TARGET_NAME
      dox
    COMMENT
      "HTML documentation"
    )
endif()

#-----------------------------------------------------------------------------
# Benchmarks
#-----------------------------------------------------------------------------
if(HERMES_BUILD_BENCHMARKS)
  message("Building benchmarks")
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/benchmarks)
endif()

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

install(
  FILES
        test/data/hermes_server.yaml
  TYPE
    DATA
  RENAME
    hermes_sample.conf
)

#-----------------------------------------------------------------------------
# Configure the hermes-config-version .cmake file for the install directory
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

add_custom_target(lint
  COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/ci/lint.sh ${CMAKE_CURRENT_SOURCE_DIR}
)
