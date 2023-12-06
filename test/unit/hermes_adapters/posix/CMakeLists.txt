include_directories(${CMAKE_SOURCE_DIR}/src/api)
include_directories(${CMAKE_SOURCE_DIR}/src)

#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------

add_executable(posix_adapter_test
        posix_adapter_test.cc
        posix_adapter_basic_test.cc
        posix_adapter_rs_test.cc
        ${ADAPTER_COMMON})
add_dependencies(posix_adapter_test
        hermes)
target_link_libraries(posix_adapter_test
        hermes)
target_compile_definitions(posix_adapter_test PUBLIC
        HERMES_MPI_TESTS=false)
jarvis_test(posix test_posix_basic)

add_executable(hermes_posix_adapter_test
        posix_adapter_test.cc
        posix_adapter_basic_test.cc
        posix_adapter_rs_test.cc
        ${ADAPTER_COMMON})
add_dependencies(hermes_posix_adapter_test
        hermes_posix)
target_link_libraries(hermes_posix_adapter_test
        hermes_posix)
target_compile_definitions(hermes_posix_adapter_test PUBLIC
        HERMES_INTERCEPT=1 HERMES_MPI_TESTS=false)
jarvis_test(posix test_hermes_posix_basic_small)
jarvis_test(posix test_hermes_posix_basic_large)

add_executable(posix_adapter_mpi_test
        posix_adapter_test.cc
        posix_adapter_basic_test.cc
        posix_adapter_rs_test.cc
        # posix_adapter_shared_test.cc
        ${ADAPTER_COMMON})
add_dependencies(posix_adapter_mpi_test
        hermes)
target_link_libraries(posix_adapter_mpi_test
        hermes Catch2::Catch2 MPI::MPI_CXX)
target_compile_definitions(posix_adapter_mpi_test PUBLIC
        HERMES_MPI_TESTS=true)
jarvis_test(posix test_posix_basic_mpi)

add_executable(hermes_posix_adapter_mpi_test
        posix_adapter_test.cc
        posix_adapter_basic_test.cc
        posix_adapter_rs_test.cc
        # posix_adapter_shared_test.cc
        ${ADAPTER_COMMON})
add_dependencies(hermes_posix_adapter_mpi_test
        hermes_posix)
target_link_libraries(hermes_posix_adapter_mpi_test
        hermes_posix Catch2::Catch2 MPI::MPI_C MPI::MPI_CXX)
target_compile_definitions(hermes_posix_adapter_mpi_test PUBLIC
        HERMES_INTERCEPT=1 HERMES_MPI_TESTS=true)
jarvis_test(posix test_hermes_posix_basic_mpi_small)
jarvis_test(posix test_hermes_posix_basic_mpi_large)

include_directories(${CMAKE_SOURCE_DIR}/adapter)
add_executable(posix_simple_io_mpi posix_simple_io_mpi.cc)
add_dependencies(posix_simple_io_mpi hermes_posix)
target_link_libraries(posix_simple_io_mpi
        hermes_posix Catch2::Catch2 MPI::MPI_CXX)

add_executable(hermes_posix_simple_io_omp posix_simple_io_omp.cc)
add_dependencies(hermes_posix_simple_io_omp hermes_posix)
target_link_libraries(hermes_posix_simple_io_omp
        hermes_posix Catch2::Catch2 OpenMP::OpenMP_CXX)

add_executable(posix_simple_io_omp posix_simple_io_omp.cc)
target_link_libraries(posix_simple_io_omp
        ${HermesShm_LIBRARIES} Catch2::Catch2 OpenMP::OpenMP_CXX)

jarvis_test(posix test_posix_simple_io_omp)
jarvis_test(posix test_hermes_posix_simple_io_omp)
#pytest(posix test_hermes_posix_simple_io_omp_default)
#pytest(posix test_hermes_posix_simple_io_omp_scratch)
#pytest(posix test_hermes_posix_simple_io_omp_preload)

set(POSIX_TESTS
  posix_adapter_test
  hermes_posix_adapter_test
  posix_adapter_mpi_test
  hermes_posix_adapter_mpi_test
  posix_simple_io_mpi
  posix_simple_io_omp
)

foreach(program ${POSIX_TESTS})
  target_include_directories(${program} PUBLIC ${HERMES_ADAPTER_DIR})
  target_include_directories(${program} PUBLIC ${HERMES_ADAPTER_TEST_DIR})
  target_link_libraries(${program} Catch2::Catch2 -lstdc++fs -lc
          MPI::MPI_CXX OpenMP::OpenMP_CXX)
endforeach()

if(HERMES_INSTALL_TESTS)
    foreach(program ${POSIX_TESTS})
        install(
                TARGETS
                ${program}
                LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
                ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
                RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
    endforeach()
endif()
