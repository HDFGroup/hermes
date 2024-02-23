set(HERMES_VFD_DIR ${HERMES_ADAPTER_DIR}/vfd)

set(hermes_vfd_tests
        hermes_vfd_adapter_test
)

add_executable(hermes_vfd_adapter_test
        ${CMAKE_CURRENT_SOURCE_DIR}/hermes_vfd_test.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/hermes_vfd_basic_test.cc)
target_include_directories(hermes_vfd_adapter_test PRIVATE ${HERMES_VFD_DIR})
target_include_directories(hermes_vfd_adapter_test PRIVATE ${HERMES_ADAPTER_TEST_DIR})
target_include_directories(hermes_vfd_adapter_test
  SYSTEM PRIVATE ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
)
target_compile_definitions(hermes_vfd_adapter_test PUBLIC
        HERMES_INTERCEPT=1 HERMES_MPI_TESTS=true)
add_dependencies(hermes_vfd_adapter_test hermes)
target_link_libraries(hermes_vfd_adapter_test
        hermes
        Catch2::Catch2
        MPI::MPI_CXX
        stdc++fs
        ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES})

jarvis_test(vfd test_hermes_vfd_basic)
jarvis_test(vfd test_hermes_vfd_scratch)


install(
    TARGETS
    hermes_vfd_adapter_test
    LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
    ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
    RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
)

file (COPY ${CMAKE_CURRENT_SOURCE_DIR}/hermes_vfd_py_test.py
        DESTINATION ${HERMES_INSTALL_BIN_DIR}
        FILE_PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ)