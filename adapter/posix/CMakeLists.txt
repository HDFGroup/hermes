include_directories(
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR})

# Create the POSIX Real API singleton
add_library(hermes_posix_singleton SHARED ${CMAKE_CURRENT_SOURCE_DIR}/posix_singleton.cc)
target_compile_options(hermes_posix_singleton PUBLIC -fPIC)

# Create the POSIX interceptor
add_library(hermes_posix SHARED ${CMAKE_CURRENT_SOURCE_DIR}/posix.cc)
add_dependencies(hermes_posix hermes hermes_posix_singleton)
target_link_libraries(hermes_posix hermes hermes_posix_singleton)
target_compile_options(hermes_posix PUBLIC -fPIC)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_posix_singleton
        hermes_posix
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(HERMES_EXPORTED_LIBS hermes_posix_singleton ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
EXPORT (
        TARGETS
        ${HERMES_EXPORTED_LIBS}
        FILE
        ${HERMES_EXPORTED_TARGETS}.cmake
)
endif()