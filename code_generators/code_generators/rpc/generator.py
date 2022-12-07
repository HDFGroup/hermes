import os
from code_generators.decorator.decorator import ApiDecorator
from code_generators.decorator.api import Api
from code_generators.util.naming import to_snake_case
from code_generators.util.conv import str_to_bool

rpc_func_text = """
{DEC}
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
{DEC}
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
rpc_lambda_text_void = """
auto {LAMBDA_NAME} = 
  [](const request &req, {PARAMS}) {{
    auto result = {LOCAL_NAME}({PASS_PARAMS});
    req.respond(result);
  }};
server_engine_->define("{GLOBAL_NAME}", {LAMBDA_NAME});
"""


class RpcGenerator(ApiDecorator):
    def __init__(self):
        """
        Automatically inserts RPC code into class files and builds
        rpc_thallium_defs.cc.
        """

        super().__init__("RPC")
        self.path = None
        self.class_instance = None
        self.rpcs = {}  # [class_path][class_name][api_name] ->
                        # tuple(local_rpc_name, target_node,
                        # class_instance)

    def set_rpc_lambda_file(self, rpc_defs_path):
        self.rpc_defs_path = rpc_defs_path

    def set_file(self, path):
        """
        Sets information used in subsequent calls to "add".

        :param path: the path to the class containing RPC prototypes
        (e.g., metadata_manager.h)
        """

        self.path = path

    def set_class(self, class_name, class_instance):
        """
        Sets information used in subsequent calls to "add".

        :param class_name: the name of the class to scan prototypes from
        (e.g., hermes::MetadataManager). Namespace is required.
        :param class_instance: The name of the C++ class instance to input
        into the RPC lambda in rpc_thallium_defs.cc (e.g., mdm). These are
        encapsulated in RPC_CLASS_INSTANCE_DEFS in rpc_thallium_defs.cc.
        :return: None
        """
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
            self.rpcs[self.path][self.class_name] = {}
        self.rpcs[self.path][self.class_name][local_rpc_name] = (
            (target_node, self.class_instance))

    def modify(self, api_map):
        """
        Generates the RPCs (GlobalRPC + lambda) for a particular class file
        (e.g., metadata_manager.h)

        :param path: the path to the C++ class file (e.g., metadata_manager.h)
        :param class_rpcs: the set of RPCs names belonging to the class
        :param rpc_lambdas: (output)
        :return: Modifies rpc_lambdas and the C++ class file.
        """
        # Get the autogen dict for rpc_thallium_defs.cc
        autogen_api_dict = api_map[self.rpc_defs_path][None]

        # Autogen RPCs for each class
        for path, namespace_dict in api_map.items():
            if path not in self.rpcs:
                continue
            for namespace, api_dict in namespace_dict.items():
                indent = api_dict['indent']
                if namespace not in self.rpcs[path]:
                    continue
                for local_rpc_api in api_dict['apis'].values():
                    if "RPC" not in local_rpc_api.decorators:
                        continue
                    if local_rpc_api.name not in self.rpcs[path][namespace]:
                        continue
                    rpc_info = self.rpcs[path][namespace][local_rpc_api.name]
                    target_node = rpc_info[0]
                    class_instance = rpc_info[1]

                    # Generate RPC code
                    global_name = local_rpc_api.name.replace("Local", "")
                    lambda_name = f"remote_{to_snake_case(global_name)}"
                    rpc_api = self.create_global_rpc_func(
                        global_name, local_rpc_api, target_node, indent)
                    rpc_lambda = self.create_rpc_lambda(
                        global_name, lambda_name,
                        local_rpc_api, class_instance, indent)

                    # Add the generated code to the API map
                    if len(rpc_api.decorators) == 1:
                        api_dict['apis'][rpc_api.name] = rpc_api
                    else:
                        api_dict['gen'].append(rpc_api.api_str)
                    autogen_api_dict['gen'].append(rpc_lambda)

    def add_space(self, space, text):
        lines = text.splitlines()
        for j, line in enumerate(lines):
            lines[j] = f"{space}{line}"
        return "\n".join(lines)

    def create_global_rpc_func(self, global_name, rpc, target_node, indent):
        func = rpc_func_text.format(
            DEC=rpc.get_decorator_macro_str("RPC"),
            RET=rpc.ret,
            LOCAL_NAME=rpc.name,
            GLOBAL_NAME=global_name,
            PARAMS=rpc.get_args(),
            PASS_PARAMS=rpc.pass_args(),
            TARGET_NODE=target_node
        ).strip()
        func = self.add_space(indent, func)
        return Api(rpc.path, rpc.namespace, func,
                   rpc.all_decorators)

    def create_rpc_lambda(self, global_name, lambda_name,
                          rpc, class_instance, indent):
        lambda_text = rpc_lambda_text
        if class_instance is None:
            lambda_text = rpc_lambda_text_void
        func = lambda_text.format(
            DEC=rpc.get_decorator_macro_str("RPC"),
            GLOBAL_NAME=global_name,
            LAMBDA_NAME=lambda_name,
            class_instance=class_instance,
            PARAMS=rpc.get_args(),
            PASS_PARAMS=rpc.pass_args(),
            LOCAL_NAME=rpc.name
        ).strip()
        func = self.add_space(indent, func)
        return func
