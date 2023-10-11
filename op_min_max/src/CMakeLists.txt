#------------------------------------------------------------------------------
# Build Small Message Task Library
#------------------------------------------------------------------------------
add_library(op_min_max SHARED
        op_min_max.cc)
add_dependencies(op_min_max ${Hermes_RUNTIME_DEPS})
target_link_libraries(op_min_max ${Hermes_RUNTIME_LIBRARIES})

#------------------------------------------------------------------------------
# Install Small Message Task Library
#------------------------------------------------------------------------------
install(
        TARGETS
        op_min_max
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
        op_min_max
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
    set_coverage_flags(op_min_max)
endif()
