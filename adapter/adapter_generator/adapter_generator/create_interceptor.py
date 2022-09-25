import sys,os
import re

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
    def __init__(self, api_str):
        self.decompose_prototype(api_str)

    def _is_text(self, tok):
        first_is_text = re.match("[_a-zA-Z]", tok[0]) is not None
        if not first_is_text:
            return False
        return re.match("[_a-zA-Z0-9]+", tok) is not None

    def _clean(self, toks):
        return [tok for tok in toks if tok is not None and len(tok) > 0]

    def get_arg_tuple(self, arg):
        arg_toks = self._clean(re.split("[ ]|(\*+)", arg))
        if len(arg_toks) == 1:
            if arg_toks[0] == '...':
                type = ""
                name = "..."
                return (type, name)
        type = " ".join(arg_toks[:-1])
        name = arg_toks[-1]
        return (type, name)

    def decompose_prototype(self, api_str):
        toks = self._clean(re.split("[()]", api_str))
        proto, args = toks[0], toks[1]

        try:
            proto = self._clean(re.split("[ ]|(\*+)", proto))
            self.name = proto[-1]
            self.ret = " ".join(proto[:-1])
            self.real_name = self.name
            self.type = f"{self.name}_t"
        except:
            print(f"Failed to decompose proto name: {proto}")
            exit()

        try:
            self.var_defs = []
            args = args.split(',')
            for arg in args:
                self.var_defs.append(self.get_arg_tuple(arg))
        except:
            print(f"Failed to decompose proto args: {args}")
            exit(1)

    def get_args(self):
        if len(self.var_defs) == 0:
            return ""
        try:
            args = [" ".join(arg_tuple) for arg_tuple in self.var_defs]
        except:
            print(f"Failed to get arg list: {self.var_defs}")
            exit(1)
        return ", ".join(args)

    def pass_args(self):
        if self.var_defs is None:
            return ""
        args = [arg[-1] for arg in self.var_defs]
        return ", ".join(args)

class ApiClass:
    def __init__(self, namespace, apis, includes, path=None, do_save=True):
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
        for include in includes:
            self.lines.append(f"#include {include}")
        self.lines.append("")

        self.lines.append(f"namespace hermes::adapter::{namespace} {{")
        self.lines.append(f"")
        self.lines.append(f"class API {{")

        self.lines.append(f" public:")
        for api in self.apis:
            self.add_intercept_api(api)

        self.lines.append(f"  API() {{")
        self.lines.append(f"    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, \"{namespace}_intercepted\");")
        for api in self.apis:
            self.init_api(api)
        self.lines.append(f"  }}")
        self.lines.append(f"}};")
        self.lines.append(f"}}  // namespace hermes::adapter::{namespace}")

        self.lines.append("")
        self.lines.append(f"#endif  // HERMES_ADAPTER_{namespace.upper()}_H")
        self.lines.append("")
        self.text = "\n".join(self.lines)

        if do_save:
            self.save(path, namespace)
        else:
            print(self.text)


    def save(self, path, namespace):
        if path is None:
            ns_dir = os.path.dirname(os.getcwd())
            path = os.path.join(ns_dir, namespace, f"real_api.h")
        with open(path, "w") as fp:
            fp.write(self.text)

    def add_intercept_api(self, api):
        self.lines.append(f"  typedef {api.ret} (*{api.type})({api.get_args()});")
        self.lines.append(f"  {api.ret} (*{api.real_name})({api.get_args()}) = nullptr;")

    def init_api(self, api):
        self.lines.append(f"    if (is_intercepted) {{")
        self.lines.append(f"      {api.real_name} = ({api.type})dlsym(RTLD_NEXT, \"{api.name}\");")
        self.lines.append(f"    }} else {{")
        self.lines.append(f"      {api.real_name} = ({api.type})dlsym(RTLD_DEFAULT, \"{api.name}\");")
        self.lines.append(f"    }}")
        self.lines.append(f"    if ({api.real_name} == nullptr) {{")
        self.lines.append(f"      LOG(FATAL) << \"HERMES Adapter failed to map symbol: \"")
        self.lines.append(f"      \"{api.name}\" << std::endl;")
        self.lines.append(f"    }}")