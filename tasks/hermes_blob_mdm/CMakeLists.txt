#------------------------------------------------------------------------------
# Build Hermes Blob MDM Task Library
#------------------------------------------------------------------------------
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(src)

#-----------------------------------------------------------------------------
# Install HRUN Admin Task Library Headers
#-----------------------------------------------------------------------------
install(DIRECTORY include DESTINATION ${CMAKE_INSTALL_PREFIX})
