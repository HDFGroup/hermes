include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
)

add_executable(mdm_bench mdm_bench.cc)
target_link_libraries(mdm_bench hermes MPI::MPI_CXX
  $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
target_compile_definitions(mdm_bench
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)

add_executable(dpe_bench dpe_bench.cc)
target_link_libraries(dpe_bench hermes MPI::MPI_CXX
  $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
target_compile_definitions(dpe_bench
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
