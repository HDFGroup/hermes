#------------------------------------------------------------------------------
# Build Small Message Task Library
#------------------------------------------------------------------------------
add_library(ram_bdev SHARED
        ram_bdev.cc)
add_dependencies(ram_bdev ${Hermes_RUNTIME_DEPS})
target_link_libraries(ram_bdev ${Hermes_RUNTIME_LIBRARIES})

#------------------------------------------------------------------------------
# Install Small Message Task Library
#------------------------------------------------------------------------------
install(
        TARGETS
        ram_bdev
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        DESTINATION
        ${HERMES_INSTALL_DATA_DIR}/cmake/hermes
        FILE
        ${HERMES_EXPORTED_TARGETS}.cmake
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(HERMES_EXPORTED_LIBS
        ram_bdev
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
    EXPORT (
            TARGETS
            ${HERMES_EXPORTED_LIBS}
            FILE
            ${HERMES_EXPORTED_TARGETS}.cmake
    )
endif()

#------------------------------------------------------------------------------
# Coverage
#------------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(ram_bdev)
endif()
