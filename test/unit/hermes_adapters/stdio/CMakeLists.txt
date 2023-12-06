include_directories(
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src)

#------------------------------------------------------------------------------
# STDIO Adapter Internal tests
#------------------------------------------------------------------------------
#add_executable(stdio_adapter_mapper_test stdio_adapter_mapper_test.cc ${ADAPTER_COMMON})
#target_link_libraries(stdio_adapter_mapper_test stdc++fs hermes_stdio)
#add_dependencies(stdio_adapter_mapper_test hermes_stdio)
#pytest(stdio test_hermes_stdio_mapper)

#------------------------------------------------------------------------------
# STDIO Adapter End to End tests
#------------------------------------------------------------------------------
# Non-Hermes, None-MPI STDIO basic
add_executable(stdio_adapter_test
        stdio_adapter_test.cc
        stdio_adapter_basic_test.cc
        stdio_adapter_func_test.cc
        stdio_adapter_rs_test.cc
        ${ADAPTER_COMMON})
add_dependencies(stdio_adapter_test
        hermes)
target_link_libraries(stdio_adapter_test
        hermes)
target_compile_definitions(stdio_adapter_test PUBLIC
        HERMES_MPI_TESTS=false)
jarvis_test(stdio test_stdio_basic)

# Hermes, None-MPI STDIO basic
add_executable(hermes_stdio_adapter_test
        stdio_adapter_test.cc
        stdio_adapter_basic_test.cc
        stdio_adapter_rs_test.cc
        ${ADAPTER_COMMON})
add_dependencies(hermes_stdio_adapter_test
        hermes_stdio)
target_link_libraries(hermes_stdio_adapter_test
        hermes_stdio)
target_compile_definitions(hermes_stdio_adapter_test PUBLIC
        HERMES_INTERCEPT=1 HERMES_MPI_TESTS=false)
jarvis_test(stdio test_hermes_stdio_basic_small)
jarvis_test(stdio test_hermes_stdio_basic_large)

# Non-Hermes, MPI STDIO basic
add_executable(stdio_adapter_mpi_test
        stdio_adapter_test.cc
        stdio_adapter_basic_test.cc
        stdio_adapter_rs_test.cc
        stdio_adapter_shared_test.cc
        ${ADAPTER_COMMON})
add_dependencies(stdio_adapter_mpi_test
        hermes)
target_link_libraries(stdio_adapter_mpi_test
        hermes)
target_compile_definitions(stdio_adapter_mpi_test PUBLIC
        HERMES_MPI_TESTS=true)
jarvis_test(stdio test_stdio_basic_mpi)

# Non-Hermes, MPI STDIO basic
add_executable(hermes_stdio_adapter_mpi_test
        stdio_adapter_test.cc
        stdio_adapter_basic_test.cc
        stdio_adapter_rs_test.cc
        stdio_adapter_shared_test.cc
        ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_adapter_mpi_test
        hermes_stdio)
add_dependencies(hermes_stdio_adapter_mpi_test
        hermes_stdio)
target_compile_definitions(hermes_stdio_adapter_mpi_test PUBLIC
        HERMES_INTERCEPT=1 HERMES_MPI_TESTS=true)
jarvis_test(stdio test_hermes_stdio_basic_mpi_small)
jarvis_test(stdio test_hermes_stdio_basic_mpi_large)
#
#add_executable(hermes_stdio_low_buf_adapter_test
#        stdio_adapter_low_buffer_space_test.cpp
#        ${ADAPTER_COMMON})
#target_link_libraries(hermes_stdio_low_buf_adapter_test hermes_stdio)
#add_dependencies(hermes_stdio_low_buf_adapter_test hermes_stdio)
#set_target_properties(hermes_stdio_low_buf_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
#pytest(stdio test_hermes_stdio_low_buf)
#
#add_executable(hermes_stdio_adapter_mode_test
#        stdio_adapter_mode_test.cpp
#        ${ADAPTER_COMMON})
#target_link_libraries(hermes_stdio_adapter_mode_test
#        hermes_stdio)
#add_dependencies(hermes_stdio_adapter_mode_test
#        hermes_stdio)
#set_target_properties(hermes_stdio_adapter_mode_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
#pytest(stdio test_hermes_stdio_bypass)
#pytest(stdio test_hermes_stdio_default)
#pytest(stdio test_hermes_stdio_scratch)

set(STDIO_TESTS
#  stdio_adapter_mapper_test
  stdio_adapter_test
  hermes_stdio_adapter_test
#  hermes_stdio_low_buf_adapter_test
#  hermes_stdio_adapter_mode_test
  stdio_adapter_mpi_test
  hermes_stdio_adapter_mpi_test
)

foreach(program ${STDIO_TESTS})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_DIR})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_TEST_DIR})
  target_link_libraries(${program} Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
endforeach()

if(HERMES_INSTALL_TESTS)
    foreach(program ${STDIO_TESTS})
        install(
                TARGETS
                ${program}
                LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
                ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
                RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
    endforeach()
endif()
