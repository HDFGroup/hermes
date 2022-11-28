add_test(NAME Teststage_in_simple
         COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/stage_file.sh
         ${MPIEXEC_EXECUTABLE}
         ${CMAKE_SOURCE_DIR}
         ${CMAKE_BINARY_DIR}/bin
         ${CMAKE_SOURCE_DIR}/test/data/staging.yaml
         2 0)