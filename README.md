# Hermes

Hermes is a heterogeneous-aware, multi-tiered, dynamic, and distributed I/O buffering system that aims to significantly accelerate I/O performance. See the [official site](http://www.cs.iit.edu/~scs/assets/projects/Hermes/Hermes.html) for more information. For design documents, architecture description, performance data, and individual component design, see the [wiki](https://github.com/HDFGroup/hermes/wiki).

![Build](https://github.com/HDFGroup/hermes/workflows/GitHub%20Actions/badge.svg)

[![Coverage Status](https://coveralls.io/repos/github/HDFGroup/hermes/badge.svg?branch=master)](https://coveralls.io/github/HDFGroup/hermes?branch=master)

## Dependencies

* A C++ compiler that supports C++ 17.
* [Thallium](https://mochi.readthedocs.io/en/latest/installing.html) - RPC library for HPC. Use a version greater than `0.5` for RoCE support.
* [yaml-cpp](https://github.com/jbeder/yaml-cpp) - YAML file parser
* MPI (tested with MPICH `3.3.2` and OpenMPI `4.0.3`). Note: The MPI-IO adapter
      only supports MPICH. If you don't need the MPI-IO adapter you can use OpenMPI,
      but you must define the CMake variable `HERMES_ENABLE_MPIIO_ADAPTER=OFF`.
* HDF5 1.14.0 if compiling with VFD

## Building

### Spack

[Spack](https://spack.io/) is the easiest way to get Hermes and all its dependencies installed.

```bash
# set location of hermes_file_staging
git clone https://github.com/HDFGroup/hermes
spack repo add ${HERMES_REPO}/ci/hermes
# Master should include all stable updates
spack install hermes@master
```

### CMake

Hermes makes use of the CMake build system and requires an out of source build.

```
cd /path/to/hermes
mkdir build
cd build
cmake ../ -DCMAKE_BUILD_TYPE=Relase -DCMAKE_INSTALL_PREFIX=...
make -j8
make install
```

## Contributing

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html). You can run `make lint` to ensure that your code conforms to the style. This requires the `cpplint` Python module (`pip install cpplint`). Alternatively, you can let the CI build inform you of required style changes.
