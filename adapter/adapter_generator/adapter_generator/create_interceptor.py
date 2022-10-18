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
        self.api_str = api_str
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
        args = [arg[-1] for arg in self.var_defs if arg[0] != '']
        return ", ".join(args)

class ApiClass:
    def __init__(self, namespace, apis, includes, dir=None,
                 create_h=True, create_cc=False):
        self.apis = apis
        h_file = None
        cc_file = None
        # Ensure that directory is set if create_cc or create_h is true
        if create_h or create_cc:
            if dir is None:
                dir = os.path.dirname(os.getcwd())
                dir = os.path.join(dir, namespace)
        if dir is not None and create_h:
            h_file = os.path.join(dir, "real_api.h")
            print(f"Will create header {h_file}")
        if dir is not None and create_cc:
            cc_file = os.path.join(dir, f"{namespace}.cc")
            print(f"Will create cpp file {h_file}")
        self.cc_lines = []
        self.h_lines = []
        self._CreateH(namespace, includes, h_file)
        self._CreateCC(namespace, cc_file)

    def _CreateCC(self, namespace, path):
        self.cc_lines.append(preamble)
        self.cc_lines.append("")

        # Includes
        self.cc_lines.append(f"bool {namespace}_intercepted = true;")
        self.cc_lines.append(f"#include \"real_api.h\"")
        self.cc_lines.append(f"#include \"singleton.h\"")
        self.cc_lines.append("")

        # Namespace simplification
        self.cc_lines.append(f"using hermes::adapter::{namespace}::API;")
        self.cc_lines.append(f"using hermes::Singleton;")
        self.cc_lines.append("")

        # Intercept function
        for api in self.apis:
            self.cc_lines.append(f"{api.api_str} {{")
            self.cc_lines.append(f"  auto real_api = Singleton<API>::GetInstance();")
            self.cc_lines.append(f"  REQUIRE_API({api.name});")
            self.cc_lines.append(f"  // auto fs_api = ")
            self.cc_lines.append(f"  return real_api->{api.name}({api.pass_args()});")
            self.cc_lines.append(f"}}")
            self.cc_lines.append("")

        text = "\n".join(self.cc_lines)
        self.save(path, text)

    def _CreateH(self, namespace, includes, path):
        self.h_lines.append(preamble)
        self.h_lines.append("")
        self.h_lines.append(f"#ifndef HERMES_ADAPTER_{namespace.upper()}_H")
        self.h_lines.append(f"#define HERMES_ADAPTER_{namespace.upper()}_H")

        # Include files
        self.h_lines.append("#include <string>")
        self.h_lines.append("#include <dlfcn.h>")
        self.h_lines.append("#include <iostream>")
        self.h_lines.append("#include <glog/logging.h>")
        for include in includes:
            self.h_lines.append(f"#include {include}")
        self.h_lines.append("")

        # Require API macro
        self.require_api()
        self.h_lines.append("")

        # Create typedefs
        self.h_lines.append(f"extern \"C\" {{")
        for api in self.apis:
            self.add_typedef(api)
        self.h_lines.append(f"}}")
        self.h_lines.append(f"")

        # Create the class definition
        self.h_lines.append(f"namespace hermes::adapter::{namespace} {{")
        self.h_lines.append(f"")
        self.h_lines.append(f"class API {{")

        # Create class function pointers
        self.h_lines.append(f" public:")
        for api in self.apis:
            self.add_intercept_api(api)
        self.h_lines.append(f"")

        # Create the symbol mapper
        self.h_lines.append(f"  API() {{")
        self.h_lines.append(f"    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, \"{namespace}_intercepted\");")
        for api in self.apis:
            self.init_api(api)
        self.h_lines.append(f"  }}")

        # Create the symbol require function
        self.h_lines.append("")

        # End the class, namespace, and header guard
        self.h_lines.append(f"}};")
        self.h_lines.append(f"}}  // namespace hermes::adapter::{namespace}")
        self.h_lines.append("")
        self.h_lines.append(f"#endif  // HERMES_ADAPTER_{namespace.upper()}_H")
        self.h_lines.append("")

        text = "\n".join(self.h_lines)
        self.save(path, text)

    def require_api(self):
        self.h_lines.append(f"#define REQUIRE_API(api_name) \\")
        self.h_lines.append(f"  if (real_api->api_name == nullptr) {{ \\")
        self.h_lines.append(f"    LOG(FATAL) << \"HERMES Adapter failed to map symbol: \" \\")
        self.h_lines.append(f"    #api_name << std::endl; \\")
        self.h_lines.append(f"    exit(1);")

    def add_typedef(self, api):
        self.h_lines.append(f"typedef {api.ret} (*{api.type})({api.get_args()});")

    def add_intercept_api(self, api):
        self.h_lines.append(f"  {api.ret} (*{api.real_name})({api.get_args()}) = nullptr;")

    def init_api(self, api):
        self.h_lines.append(f"    if (is_intercepted) {{")
        self.h_lines.append(f"      {api.real_name} = ({api.type})dlsym(RTLD_NEXT, \"{api.name}\");")
        self.h_lines.append(f"    }} else {{")
        self.h_lines.append(f"      {api.real_name} = ({api.type})dlsym(RTLD_DEFAULT, \"{api.name}\");")
        self.h_lines.append(f"    }}")

    def save(self, path, text):
        if path is None:
            print(text)
            return
        with open(path, "w") as fp:
            fp.write(text)