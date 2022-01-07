# HDF5 Hermes VFD
## 1. Description
The HDF5 Hermes VFD is a Virtual File Driver (VFD) for HDF5 that can be used to interface with Hermes) API. The driver is built as a plugin library that is external to HDF5.

## 2. Dependencies
To build the HDF5 Hermes VFD, the following libraries are required:
* [Hermes](https://github.com/HDFGroup/hermes) - A heterogeneous aware, multi-tiered, dynamic, and distributed I/O buffering system that aims to significantly accelerate I/O performance. Make sure to build with HERMES_ENABLE_WRAPPER=ON.
* [HDF5](https://github.com/HDFGroup/hdf5) - HDF5 is built for fast I/O processing and storage.

## 3. Building

### CMake
Hermes VFD makes use of the CMake build system and requires an out of source build.
```
cd /path/to/Hermes_VFD
mkdir build
cd build
ccmake ..
```

Type 'c' to configure until there are no errors, then generate the makefile with 'g'. The default options should suffice for most use cases. In addition, we recommend the following options.

```
-DCMAKE_INSTALL_PREFIX=/installation/prefix
-DHDF5_DIR=/paht/to/hdf5
-DHERMES_DIR=/path/to/hermes
```
After the makefile has been generated, you can type `make -j 2` or `cmake --build . -- -j 2`. Add `VERBOSE=1` to see detailed compiler output.

Assuming that the `CMAKE_INSTALL_PREFIX` has been set and that you have write permissions to the destination directory, you can install the driver by simply doing:
```
make install
```

## 4. Usage
To use the HDF5 Hermes VFD in an HDF5 application, the driver can either be linked into the application, or it can be dynamically loaded as a plugin. If dynamically loading the Hermes VFD, users should ensure that the HDF5_PLUGIN_PATH environment variable points to the directory containing the built VFD library if the VFD has been installed to a non-standard location.

### Method 1: Linked into application
To link the Hermes VFD into an HDF5 application, the application should include the H5FDhermes.h header that gets installed on the system and should link the installed VFD library (libhdf5_hermes_vfd.so) into the application. Once this has been done, Hermes VFD access can be setup by calling `H5Pset_fapl_hermes(...)` on a FAPL within the HDF5 application. The test `hermes_set_fapl` is using this method, in which it calls `H5Pset_fapl_hermes` directly.

### Method 2: Dynamically loaded by FAPL
To explicitly load the Hermes VFD inside an HDF5 application, a call to the `H5Pset_driver_by_name(...)` routine should be made to setup Hermes VFD access on a FAPL. This will cause HDF5 to load the VFD as a plugin and set the VFD on the given FAPL. The string "hermes" should be given for the driver_name parameter. A string should be given for the `driver_config` parameter (last parameter in `H5Pset_driver_by_name`), as the driver requires additional parameters to config Hermes. An example string like "false,1024" (comma dilemma) is matching the second and third parameter as in `H5Pset_fapl_hermes`. User also needs to set up the enrivonment variable `HDF5_PLUGIN_PATH`, which points to directory containing the built Hermes VFD library. Test `hermes_driver` is using this method.

### Method 3: Dynamically loaded by environment variable
To implicitly load the Hermes VFD inside an HDF5 application, the HDF5_DRIVER environment variable may be set to the string "hermes". During library initialization, HDF5 will check this environment variable, load the Hermes VFD as a plugin and set the VFD as the default file driver on File Access Property Lists. Therefore, any file access that uses H5P_DEFAULT for its FAPL, or which uses a FAPL that hasn't had a specific VFD set on it, will automatically use the Hermes VFD for file access. User can simply setup HDF5_DRIVER environment variable to "hermes" and HDF5_DRIVER_CONFIG the same as `driver_config` parameter without calling `H5Pset_driver_by_name()`, compared to Method 2 Dynamically loaded by FAPL. Test `hermes_env` is using this method.

## 5. More Information
[More about Hermes](https://github.com/HDFGroup/hermes/wiki)

[HDF5 VFD Plugins RFC](https://github.com/HDFGroup/hdf5doc/blob/master/RFCs/HDF5_Library/VFL_DriverPlugins/RFC__A_Plugin_Interface_for_HDF5_Virtual_File_Drivers.pdf)
