#------------------------------------------------------------------------------
# Build Hrun Admin Task Library
#------------------------------------------------------------------------------
include_directories(include)
add_subdirectory(src)

#-----------------------------------------------------------------------------
# Install HRUN Admin Task Library Headers
#-----------------------------------------------------------------------------
install(DIRECTORY include DESTINATION ${CMAKE_INSTALL_PREFIX})
