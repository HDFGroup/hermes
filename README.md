# Hermes

Hermes is a heterogeneous-aware, multi-tiered, dynamic, and distributed I/O buffering system that aims to significantly accelerate I/O performance. See the [official site](http://www.cs.iit.edu/~scs/assets/projects/Hermes/Hermes.html) for more information. For design documents, architecture description, performance data, and individual component design, see the [wiki](https://github.com/HDFGroup/hermes/wiki).

![Build](https://github.com/HDFGroup/hermes/workflows/GitHub%20Actions/badge.svg)

[![Coverage Status](https://coveralls.io/repos/github/HDFGroup/hermes/badge.svg?branch=master)](https://coveralls.io/github/HDFGroup/hermes?branch=master)

## Dependencies

*   A C++ compiler that supports C++ 17.
*   [Thallium](https://mochi.readthedocs.io/en/latest/installing.html) - RPC library for HPC. Use a version greater than `0.5` for RoCE support.
*   [GLOG](https://github.com/google/glog) - The Google logging library.
*   [GLPK](https://www.gnu.org/software/glpk/) - GNU Linear Programming Kit
*   MPI (tested with MPICH `3.3.2` and OpenMPI `4.0.3`). Note: The MPI-IO adapter
    only supports MPICH. If you don't need the MPI-IO adapter you can use OpenMPI,
    but you must define the CMake variable `HERMES_ENABLE_MPIIO_ADAPTER=OFF`.
*   The [Catch2](https://github.com/catchorg/Catch2) testing framework
    (only required if built with `-DBUILD_TESTING=ON`; tested with 3.0.1)

## Building

### Spack

[Spack](https://spack.io/) is the easiest way to get Hermes and all its dependencies installed.

```bash
# Install spack if you don't already have it
SPACK_DIR=~/spack
git clone https://github.com/spack/spack ${SPACK_DIR}
. ${SPACK_DIR}/share/spack/setup-env.sh
spack install hermes
```

If this doesn't work, the Hermes and Mochi spack recipes might be out of sync
with the version of spack you're using. In that case, you can try the most
up-to-date repos.

```bash
# set location of hermes_file_staging
STAGE_DIR=~/hermes_stage
MOCHI_REPO=${STAGE_DIR}/mochi
HERMES_REPO=${STAGE_DIR}/hermes
git clone https://github.com/mochi-hpc/mochi-spack-packages.git ${MOCHI_REPO}
git clone https://github.com/HDFGroup/hermes ${HERMES_REPO}
spack repo add ${MOCHI_REPO}
spack repo add ${HERMES_REPO}/ci/hermes
spack install hermes
```

### CMake

Hermes makes use of the CMake build system and requires an out of source build.

    cd /path/to/hermes
    mkdir build
    cd build
    ccmake ..

Type 'c' to configure until there are no errors, then generate the makefile with 'g'. The default options should suffice for most use cases. In addition, we recommend the following options.

    -DCMAKE_INSTALL_PREFIX=/installation/prefix
    -DCMAKE_PREFIX_PATH=/path/to/dependencies
    -DCMAKE_BUILD_RPATH=/path/to/dependencies/lib
    -DCMAKE_INSTALL_RPATH=/path/to/dependencies/lib
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_CXX_COMPILER=`which mpicxx`
    -DBUILD_SHARED_LIBS=ON
    -DORTOOLS_DIR=/path/to/ortools

After the makefile has been generated, you can type `make -j 4` or `cmake --build . -- -j 4`. Add `VERBOSE=1` to see detailed compiler output.

### Testing and Installation

After successfully building Hermes, it's a good idea to run the test suite via the command `ctest .`. This should be run from the build directory.

Finally, install the library with `make install`. You can find a complete example build script [here](https://github.com/HDFGroup/hermes/blob/master/ci/install_hermes.sh).

## Contributing

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html). You can run `make lint` to ensure that your code conforms to the style. This requires the `cpplint` Python module (`pip install cpplint`). Alternatively, you can let the CI build inform you of required style changes.
