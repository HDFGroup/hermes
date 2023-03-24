set(HERMES_VFD_DIR ${HERMES_ADAPTER_DIR}/vfd)

set(hermes_vfd_tests
  hermes_vfd_test
)

add_executable(hermes_vfd_test ${CMAKE_CURRENT_SOURCE_DIR}/hermes_vfd_test.cc)
target_include_directories(hermes_vfd_test PRIVATE ${HERMES_VFD_DIR})
target_include_directories(hermes_vfd_test PRIVATE ${HERMES_ADAPTER_TEST_DIR})
target_include_directories(hermes_vfd_test
  SYSTEM PRIVATE ${HDF5_HERMES_VFD_EXT_INCLUDE_DEPENDENCIES}
)

target_link_libraries(hermes_vfd_test
        hermes
        Catch2::Catch2
        MPI::MPI_CXX
        glog::glog
        stdc++fs
        ${HDF5_HERMES_VFD_EXT_LIB_DEPENDENCIES})

preload_vfd("vfd" kDefault "${CMAKE_BINARY_DIR}/bin/hermes_vfd_test"
        "--reporter compact -d yes")
preload_vfd("vfd_scratch_mode" kScratch "${CMAKE_BINARY_DIR}/bin/hermes_vfd_test"
        "[scratch] --reporter compact -d yes")
