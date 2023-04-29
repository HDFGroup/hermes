
#-----------------------------------------------------------------------------
# Find Java JNI
#-----------------------------------------------------------------------------
find_package(JNI REQUIRED)
find_package(Java REQUIRED)
include(UseJava)
message("Found JNI at ${JNI_INCLUDE_DIRS} and ${JNI_LIBRARIES}")

#-----------------------------------------------------------------------------
# Build C++ Java wrapper
#-----------------------------------------------------------------------------
function(java_wrapper name path)
    include_directories(${JNI_INCLUDE_DIRS})
    add_library(${name} ${path})
    add_dependencies(${name} hermes hermes_c_wrapper)
    target_link_libraries(${name}
            hermes hermes_c_wrapper
            MPI::MPI_CXX stdc++fs dl ${JNI_LIBRARIES})
endfunction()
java_wrapper(hermes_src_main_Blob src/main/cpp/src_main_Blob.cc)
java_wrapper(hermes_src_main_Bucket src/main/cpp/src_main_Bucket.cc)
java_wrapper(hermes_src_main_Hermes src/main/cpp/src_main_Hermes.cc)

#-----------------------------------------------------------------------------
# Build Java API
#-----------------------------------------------------------------------------
add_custom_command(TARGET hermes_src_main_Hermes POST_BUILD
        COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} &&
                ${CMAKE_CURRENT_SOURCE_DIR}/gradlew build -x test)
add_custom_target(build_hermes_java
        COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} &&
                ${CMAKE_CURRENT_SOURCE_DIR}/gradlew build -x test)
add_test(test_hermes_java_wrapper
        cd ${CMAKE_CURRENT_SOURCE_DIR} &&
        ${CMAKE_CURRENT_SOURCE_DIR}/gradlew test)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(
        TARGETS
        hermes_src_main_Blob
        hermes_src_main_Bucket
        hermes_src_main_Hermes
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
        hermes_src_main_Blob
        hermes_src_main_Bucket
        hermes_src_main_Hermes
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
    set_coverage_flags(hermes_src_main_Blob)
    set_coverage_flags(hermes_src_main_Bucket)
    set_coverage_flags(hermes_src_main_Hermes)
endif()
