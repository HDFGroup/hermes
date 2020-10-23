
add_executable(mdm_bench mdm_bench.cc)
target_link_libraries(mdm_bench hermes MPI::MPI_CXX
  $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
target_compile_definitions(mdm_bench
  PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
