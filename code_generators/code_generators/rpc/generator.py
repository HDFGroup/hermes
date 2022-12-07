import os
from code_generators.decorator.decorator import ApiDecorator
from code_generators.decorator.api import Api
from code_generators.util.naming import to_snake_case
from code_generators.util.conv import str_to_bool
from .parse_docstring import RpcDocstring


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
    def __init__(self, rpc_defs_path):
        """
        Automatically inserts RPC code into class files and builds
        rpc_thallium_defs.cc.
        """

        super().__init__("RPC")
        self.path = None
        self.class_instance = None
        self.rpc_defs_path = rpc_defs_path

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
        autogen_api_dict = api_map[self.rpc_defs_path]['hermes']

        # Autogen RPCs for each class
        for path, namespace_dict in api_map.items():
            for namespace, api_dict in namespace_dict.items():
                indent = api_dict['indent']
                for local_rpc_api in api_dict['apis'].values():
                    if "RPC" not in local_rpc_api.decorators:
                        continue
                    doc = RpcDocstring(local_rpc_api)
                    doc.parse()
                    target_node = doc.target_node
                    class_instance = doc.class_instance

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
