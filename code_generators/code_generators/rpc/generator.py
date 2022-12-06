import sys, os, re
from code_generators.util.api import Api, ParseDecoratedCppApis
from code_generators.util.naming import to_snake_case
from code_generators.util.conv import str_to_bool

rpc_func_text = """
{RET} {GLOBAL_NAME}({PARAMS}) {{
  u32 target_node = {TARGET_NODE};
  if (target_node == rpc_->node_id_) {{
    return {LOCAL_NAME}(
      {PASS_PARAMS});
  }} else {{
    return rpc_->Call<{RET}>(
      target_node, "{GLOBAL_NAME}",
      {PASS_PARAMS});
  }}
}}
"""

rpc_func_text2 = """
{RET} {GLOBAL_NAME}({PARAMS}) {{
  u32 target_node = {TARGET_NODE};
  return rpc_->Call<{RET}>(
      target_node, "{GLOBAL_NAME}",
      {PASS_PARAMS});
}}
"""

# Will add this to rpc_thallium.cc
rpc_lambda_text = """
auto {LAMBDA_NAME} = 
  [{class_instance}](const request &req, {PARAMS}) {{
    auto result = {class_instance}->{LOCAL_NAME}({PASS_PARAMS});
    req.respond(result);
  }};
server_engine_->define("{GLOBAL_NAME}", {LAMBDA_NAME});
"""

class RpcGenerator:
    def __init__(self, rpc_defs_path, modify=True):
        """
        Automatically inserts RPC code into class files and builds
        rpc_thallium_defs.cc.

        :param modify: Whether or not to modify class files directly or
        store temporary output in the working directory.
        """

        self.modify = str_to_bool(modify)
        self.path = None
        self.rpc_defs_path = rpc_defs_path
        self.macro = None
        self.class_instance = None
        self.rpcs = {}  # [class_path][class_name] ->
                        # tuple(local_rpc_name, target_node,
                        # class_instance)

    def set_file(self, path, class_name, class_instance):
        """
        Sets information used in subsequent calls to "add".
        
        :param path: the path to the class containing RPC prototypes 
        (e.g., metadata_manager.h) 
        :param class_name: the name of the class to scan prototypes from
        (e.g., hermes::MetadataManager). Namespace is required.
        :param class_instance: The name of the C++ class instance to input
        into the RPC lambda in rpc_thallium_defs.cc (e.g., mdm). These are
        encapsulated in RPC_CLASS_INSTANCE_DEFS in rpc_thallium_defs.cc.
        :return: None
        """
        
        self.path = path
        self.class_name = class_name
        self.class_instance = class_instance

    def add(self, local_rpc_name, target_node):
        """
        Register an RPC to scan for.

        :param local_rpc_name: The local name of the RPC in a C++ class
        (e.g., LocalGetOrCreateBucket)
        :param target_node: The C++ code to determine the node to execute an RPC
        :return: None
        """

        if self.path not in self.rpcs:
            self.rpcs[self.path] = {}
        if self.class_name not in self.rpcs[self.path]:
            self.rpcs[self.path][self.class_name] = []
        self.rpcs[self.path][self.class_name].append(
            (local_rpc_name, target_node, self.class_instance))

    def generate(self):
        """
        Generate the RPCs based on the local function prototypes.

        :return: None.
        """

        rpc_lambdas = []
        for path, class_rpcs in self.rpcs.items():
            self.generate_class_file(path, class_rpcs, rpc_lambdas)
        self.generate_rpc_file(rpc_lambdas)

    def generate_class_file(self, path, class_rpcs, rpc_lambdas):
        """
        Generates the RPCs (GlobalRPC + lambda) for a particular class file
        (e.g., metadata_manager.h)
        
        :param path: the path to the C++ class file (e.g., metadata_manager.h)
        :param class_rpcs: the set of RPCs names belonging to the class
        :param rpc_lambdas: (output)
        :return: Modifies rpc_lambdas and the C++ class file.
        """

        # Parse class file (e.g., metadata_manager.h)
        gen = ParseDecoratedCppApis(path,
                                    api_dec='RPC',
                                    autogen_dec='RPC_AUTOGEN')
        gen.parse()
        rpc_map = gen.api_map
        class_lines = gen.orig_class_lines

        # Autogen RPCs for each class
        for class_name, rpcs in class_rpcs.items():
            # Get the autogen start
            gen_start = rpc_map[class_name]['start']
            gen_end = rpc_map[class_name]['end']
            indent = rpc_map[class_name]['indent']
            global_rpc_funcs = []

            # Create lambda + global RPC
            for rpc in rpcs:
                local_rpc_name = rpc[0]
                target_node = rpc[1]
                class_instance = rpc[2]
                local_rpc_api = rpc_map[class_name]['apis'][local_rpc_name]
                self.create_global_rpc_func(local_rpc_api, target_node,
                                            indent, global_rpc_funcs)
                self.create_rpc_lambda(local_rpc_api, class_instance,
                                       indent, rpc_lambdas)

            # Remove previous autogen data
            class_lines = class_lines[:gen_start+1] + class_lines[gen_end:]

            # Insert new autogen data
            class_lines = class_lines[:gen_start + 1] + \
                          global_rpc_funcs + \
                          class_lines[gen_start + 1:]

        # Persist
        if self.modify:
            with open(path, 'w') as fp:
                fp.write("\n".join(class_lines))
        else:
            tmp_path = os.path.basename(path)
            with open(f"tmp_{tmp_path}", 'w') as fp:
                fp.write("\n".join(class_lines))

    def generate_rpc_file(self, rpc_lambdas):
        """
        Generate rpc_thallium_defs.cc

        :param rpc_lambdas:
        :return:
        """

        # Parse rpc_thallium_defs.cc
        path = self.rpc_defs_path
        gen = ParseDecoratedCppApis(path,
                                    api_dec='RPC',
                                    autogen_dec='RPC_AUTOGEN',
                                    ignore_autogen_ns=True)
        gen.parse()

        # Get the autogen start and end
        rpc_lines = gen.orig_class_lines
        gen_start = gen.api_map[None]['start']
        gen_end = gen.api_map[None]['end']

        # Remove previous autogen data
        rpc_lines = rpc_lines[:gen_start+1] + rpc_lines[gen_end:]

        # Insert new autogen data
        rpc_lines = rpc_lines[:gen_start+1] + \
                    rpc_lambdas + \
                    rpc_lines[gen_start+1:]

        # Persist
        if self.modify:
            with open(path, 'w') as fp:
                fp.write("\n".join(rpc_lines))
        else:
            tmp_path = os.path.basename(path)
            with open(tmp_path, 'w') as fp:
                fp.write("\n".join(rpc_lines))

    def add_space(self, space, text):
        lines = text.splitlines()
        for j, line in enumerate(lines):
            lines[j] = f"{space}{line}"
        return "\n".join(lines)

    def create_global_rpc_func(self, rpc, target_node, indent, lines):
        lines.append(rpc_func_text.format(
            RET=rpc.ret,
            LOCAL_NAME=rpc.name,
            GLOBAL_NAME=rpc.global_name,
            PARAMS=rpc.get_args(),
            PASS_PARAMS=rpc.pass_args(),
            TARGET_NODE=target_node
        ).strip())
        lines[-1] = self.add_space(indent, lines[-1])

    def create_rpc_lambda(self, rpc, class_instance, indent, lines):
        lines.append(rpc_lambda_text.format(
            GLOBAL_NAME=rpc.global_name,
            LAMBDA_NAME=rpc.lambda_name,
            class_instance=class_instance,
            PARAMS=rpc.get_args(),
            PASS_PARAMS=rpc.pass_args(),
            LOCAL_NAME=rpc.name
        ).strip())
        lines[-1] = self.add_space(indent, lines[-1])

