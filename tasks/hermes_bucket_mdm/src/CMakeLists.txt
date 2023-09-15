#------------------------------------------------------------------------------
# Build Small Message Task Library
#------------------------------------------------------------------------------
add_library(hermes_bucket_mdm SHARED
        hermes_bucket_mdm.cc)
add_dependencies(hermes_bucket_mdm ${Labstor_RUNTIME_DEPS})
target_link_libraries(hermes_bucket_mdm ${Labstor_RUNTIME_LIBRARIES})

#------------------------------------------------------------------------------
# Install Small Message Task Library
#------------------------------------------------------------------------------
install(
        TARGETS
        hermes_bucket_mdm
        EXPORT
        ${LABSTOR_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${LABSTOR_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${LABSTOR_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
        EXPORT
        ${LABSTOR_EXPORTED_TARGETS}
        DESTINATION
        ${LABSTOR_INSTALL_DATA_DIR}/cmake/hermes
        FILE
        ${LABSTOR_EXPORTED_TARGETS}.cmake
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(LABSTOR_EXPORTED_LIBS
        hermes_bucket_mdm
        ${LABSTOR_EXPORTED_LIBS})
if(NOT LABSTOR_EXTERNALLY_CONFIGURED)
    EXPORT (
            TARGETS
            ${LABSTOR_EXPORTED_LIBS}
            FILE
            ${LABSTOR_EXPORTED_TARGETS}.cmake
    )
endif()

#------------------------------------------------------------------------------
# Coverage
#------------------------------------------------------------------------------
if(LABSTOR_ENABLE_COVERAGE)
    set_coverage_flags(hermes_bucket_mdm)
endif()
