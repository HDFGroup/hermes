

# Creates an example trait
add_library(hermes_c_wrapper SHARED
        c_wrapper.cc)
add_dependencies(hermes_c_wrapper hermes)
target_link_libraries(hermes_c_wrapper
        hermes MPI::MPI_CXX stdc++fs dl)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_c_wrapper
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)
#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(HERMES_EXPORTED_LIBS
        hermes_c_wrapper
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
    EXPORT (
            TARGETS
            ${HERMES_EXPORTED_LIBS}
            FILE
            ${HERMES_EXPORTED_TARGETS}.cmake
    )
endif()
#-----------------------------------------------------------------------------
# Add Target(s) to Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_example_trait)
endif()
