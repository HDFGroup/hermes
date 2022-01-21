#------------------------------------------------------------------------------
# PubSub Adapter tests
#------------------------------------------------------------------------------
add_executable(pubsub_metadata_test ${CMAKE_CURRENT_SOURCE_DIR}/pubsub_metadata_test.cc)
add_test(NAME "pubsub_metadata_test" COMMAND "${CMAKE_BINARY_DIR}/bin/pubsub_metadata_test")
target_link_libraries(pubsub_metadata_test ${LIBRT} hermes MPI::MPI_CXX
        $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> hermes_pubsub)
add_dependencies(pubsub_metadata_test hermes_pubsub)

set(PUBSUB_TESTS pubsub_topic_test
        pubsub_end_to_end_test pubsub_end_to_end_test_sync)
set(EXTENDED_PUBSUB_TESTS ${PUBSUB_TESTS} pubsub_metadata_test)
foreach(program ${PUBSUB_TESTS})
    add_executable(${program} ${CMAKE_CURRENT_SOURCE_DIR}/${program}.cc)
    add_dependencies(${program} hermes_pubsub)
    target_link_libraries(${program} ${LIBRT} hermes MPI::MPI_CXX
            $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> hermes_pubsub)
    target_compile_definitions(${program}
            PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
    mpi_daemon(${program} 1 "" "" 1)
endforeach()

foreach(program ${EXTENDED_PUBSUB_TESTS})
    target_include_directories(${program} PUBLIC ${HERMES_ADAPTER_DIR})
    target_include_directories(${program} PUBLIC ${HERMES_ADAPTER_TEST_DIR})
endforeach()

if(HERMES_INSTALL_TESTS)
    set(PUBSUB_TESTS pubsub_metadata_test pubsub_topic_test
            pubsub_end_to_end_test pubsub_end_to_end_test_sync)
    foreach(program ${PUBSUB_TESTS})
        install(
                TARGETS
                ${program}
                EXPORT
                ${HERMES_EXPORTED_TARGETS}
                LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
                ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
                RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
    endforeach()
endif()