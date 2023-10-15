# find_package(PkgConfig REQUIRED)
# pkg_search_module(libfabric REQUIRED libfabric)
# message("LibFabric found at: ${libfabric_INCLUDE_DIRS} with libs ${libfabric_LIBRARIES} in dir ${libfabric_LIBRARY_DIRS}")

#------------------------------------------------------------------------------
# Build Hrun Admin Task Library
#------------------------------------------------------------------------------
include_directories(include)
add_subdirectory(src)

#-----------------------------------------------------------------------------
# Install HRUN Admin Task Library Headers
#-----------------------------------------------------------------------------
install(DIRECTORY include DESTINATION ${CMAKE_INSTALL_PREFIX})
