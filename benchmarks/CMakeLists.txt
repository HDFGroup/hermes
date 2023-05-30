include_directories(
  ${CMAKE_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/src/api
  ${PROJECT_SOURCE_DIR}/test
  ${PROJECT_SOURCE_DIR}/io_client
)

set(BENCHMARKS
        api_bench
        memcpy_bench
        reorganize)

foreach(benchmark ${BENCHMARKS})
  message("Building ${benchmark}")
  add_executable(${benchmark} ${benchmark}.cc)
  add_dependencies(${benchmark} hermes hermes_prefetcher_trait)
  target_link_libraries(${benchmark} hermes hermes_prefetcher_trait
          MPI::MPI_CXX
          $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
endforeach()
