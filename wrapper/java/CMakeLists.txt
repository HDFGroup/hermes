
#-----------------------------------------------------------------------------
# Find Java JNI
#-----------------------------------------------------------------------------
find_package(JNI REQUIRED)
message("Found JNI at ${JNI_INCLUDE_DIRS} and ${JNI_LIBRARIES}")

#-----------------------------------------------------------------------------
# Build C++ Java wrapper
#-----------------------------------------------------------------------------
include_directories(${JNI_INCLUDE_DIRS})
add_library(hermes_java_wrapper
        java_wrapper.cc)
add_dependencies(hermes_java_wrapper hermes hermes_c_wrapper)
target_link_libraries(hermes_java_wrapper
        hermes hermes_c_wrapper
        MPI::MPI_CXX stdc++fs dl ${JNI_LIBRARIES})

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_java_wrapper
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
        hermes_java_wrapper
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
