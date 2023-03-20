#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        FILES
        filesystem_io_client.h
        DESTINATION
        ${HERMES_INSTALL_INCLUDE_DIR}
        COMPONENT
        headers
)