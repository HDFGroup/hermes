#------------------------------------------------------------------------------
# Build Labstor Admin Task Library
#------------------------------------------------------------------------------
include_directories(include)
add_subdirectory(src)

#-----------------------------------------------------------------------------
# Install LabStor Admin Task Library Headers
#-----------------------------------------------------------------------------
install(DIRECTORY include DESTINATION ${CMAKE_INSTALL_PREFIX})
