include_directories(
  ${PROJECT_SOURCE_DIR}/src/api
)

set(BENCHMARKS mdm_bench dpe_bench vpic_bench)

foreach(benchmark ${BENCHMARKS})
  add_executable(${benchmark} ${benchmark}.cc)
  target_link_libraries(${benchmark} hermes MPI::MPI_CXX
    $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
  target_compile_definitions(${benchmark}
    PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
endforeach()
