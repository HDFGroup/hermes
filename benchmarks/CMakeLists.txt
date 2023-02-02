include_directories(
  ${CMAKE_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/src/api
  ${PROJECT_SOURCE_DIR}/test
)

set(BENCHMARKS put_get_bench)

foreach(benchmark ${BENCHMARKS})
  add_executable(${benchmark} ${benchmark}.cc)
  add_dependencies(${benchmark} hermes)
  target_link_libraries(${benchmark} hermes MPI::MPI_CXX
    $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> glog::glog)
endforeach()
