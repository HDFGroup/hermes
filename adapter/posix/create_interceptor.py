import sys,os

preamble = """/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */"""

class Api:
    def __init__(self, ret, name, var_defs):
        self.ret = ret
        self.name = name
        self.real_name = f"{self.name}"
        self.type = f"{self.name}_t"
        self.var_defs = var_defs

    def get_args(self):
        if self.var_defs is None:
            return ""
        args = [" ".join(arg) for arg in self.var_defs]
        return ", ".join(args)

    def pass_args(self):
        if self.var_defs is None:
            return ""
        args = [arg[-1] for arg in self.var_defs]
        return ", ".join(args)

class ApiClass:
    def __init__(self, namespace, apis, path=None):
        if path is None:
            path = os.path.join(os.getcwd(), f"{namespace}.h")
        self.apis = apis
        self.lines = []

        self.lines.append(preamble)
        self.lines.append("")
        self.lines.append(f"#ifndef HERMES_ADAPTER_{namespace.upper()}_H")
        self.lines.append(f"#define HERMES_ADAPTER_{namespace.upper()}_H")

        self.lines.append("#include <string>")
        self.lines.append("#include <dlfcn.h>")
        self.lines.append("#include <iostream>")
        self.lines.append("#include <glog/logging.h>")
        self.lines.append("#include \"interceptor.h\"")
        self.lines.append("#include \"filesystem/filesystem.h\"")
        self.lines.append("")

        self.lines.append(f"namespace hermes::adapter::{namespace} {{")
        self.lines.append(f"")
        self.lines.append(f"class API {{")

        self.lines.append(f" public:")
        for api in self.apis:
            self.add_intercept_api(api)

        #self.lines.append(f" public:")
        self.lines.append(f"  API() {{")
        self.lines.append(f"    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, \"{namespace}_intercepted\");")
        for api in self.apis:
            self.init_api(api)
        self.lines.append(f"  }}")
        self.lines.append(f"}};")
        self.lines.append(f"API real_api;")
        self.lines.append(f"}}  // namespace hermes::adapter::{namespace}")

        self.lines.append("")
        self.lines.append(f"#endif  // HERMES_ADAPTER_{namespace.upper()}_H")
        self.lines.append("")
        self.save(path)

    def save(self, path):
        text = "\n".join(self.lines)
        with open(path, "w") as fp:
            fp.write(text)

    def add_intercept_api(self, api):
        self.lines.append(f"  typedef {api.ret}(*{api.type})({api.get_args()});")
        self.lines.append(f"  {api.ret}(*{api.real_name})({api.get_args()}) = nullptr;")

    def init_api(self, api):
        self.lines.append(f"    if (is_intercepted) {{")
        self.lines.append(f"      {api.real_name} = ({api.type})dlsym(RTLD_NEXT, \"{api.name}\");")
        self.lines.append(f"      if ({api.real_name} == )")
        self.lines.append(f"    }} else {{")
        self.lines.append(f"      {api.real_name} = ({api.type})dlsym(RTLD_DEFAULT, \"{api.name}\");")
        self.lines.append(f"    }}")
        self.lines.append(f"    if ({api.real_name} == nullptr) {{")
        self.lines.append(f"      LOG(FATAL) << \"HERMES Adapter failed to map symbol: \"")
        self.lines.append(f"      \"{api.name}\" << std::endl;")
        self.lines.append(f"    }}")

apis = [
    Api("int", "open", [
        ("const", "char *", "path"),
        ("int", "flags"),
        ("", "...")
    ]),
    Api("int", "open64", [
        ("const", "char *", "path"),
        ("int", "flags"),
        ("", "...")
    ]),
    Api("int", "__open_2", [
        ("const", "char *", "path"),
        ("int", "oflag")
    ]),
    Api("int", "creat", [
        ("const", "char *", "path"),
        ("mode_t", "mode")
    ]),
    Api("int", "creat64", [
        ("const", "char *", "path"),
        ("mode_t", "mode")
    ]),
    Api("ssize_t", "read", [
        ("int", "fd"),
        ("void *", "buf"),
        ("size_t", "count")
    ]),
    Api("ssize_t", "write", [
        ("int", "fd"),
        ("const", "void *", "buf"),
        ("size_t", "count")
    ]),
    Api("ssize_t", "pread", [
        ("int", "fd"),
        ("void *", "buf"),
        ("size_t", "count"),
        ("off_t", "offset")
    ]),
    Api("ssize_t", "pwrite", [
        ("int", "fd"),
        ("const", "void *", "buf"),
        ("size_t", "count"),
        ("off_t", "offset")
    ]),
    Api("ssize_t", "pread64", [
        ("int", "fd"),
        ("void *", "buf"),
        ("size_t", "count"),
        ("off64_t", "offset")
    ]),
    Api("ssize_t", "pwrite64", [
        ("int", "fd"),
        ("const", "void *", "buf"),
        ("size_t", "count"),
        ("off64_t", "offset")
    ]),
    Api("off_t", "lseek", [
        ("int", "fd"),
        ("off_t", "offset"),
        ("int", "whence")
    ]),
    Api("off64_t", "lseek64", [
        ("int", "fd"),
        ("off64_t", "offset"),
        ("int", "whence")
    ]),
    Api("int", "__fxstat", [
        ("int", "version"),
        ("int", "fd"),
        ("struct stat *", "buf")
    ]),
    Api("int", "fsync", [
        ("int", "fd")
    ]),
    Api("int", "fdatasync", [
        ("int", "fd")
    ]),
    Api("int", "close", [
        ("int", "fd")
    ]),
    Api("int", "MPI_Init", [
        ("int *", "argc"),
        ("char ***", "argv")
    ]),
    Api("int", "MPI_Finalize", None),
]

ApiClass("posix", apis)