set(HERMES_VFD_DIR ${HERMES_ADAPTER_DIR}/vfd)

set(hermes_vfd_tests
        hermes_vfd_adapter_test
)

add_executable(hermes_vfd_adapter_test ${CMAKE_CURRENT_SOURCE_DIR}/hermes_vfd_test.cc)
target_include_directories(hermes_vfd_adapter_test PRIVATE ${HERMES_VFD_DIR})
target_include_directories(hermes_vfd_adapter_test PRIVATE ${HERMES_ADAPTER_TEST_DIR})
target_include_directories(hermes_vfd_adapter_test
  SYSTEM PRIVATE ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
)
set_target_properties(hermes_vfd_adapter_test
        PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
add_dependencies(hermes_vfd_adapter_test hermes)
target_link_libraries(hermes_vfd_adapter_test
        hermes
        Catch2::Catch2
        MPI::MPI_CXX
        stdc++fs
        ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES})

pytest(vfd test_hermes_vfd_default)
pytest(vfd test_hermes_vfd_scratch)