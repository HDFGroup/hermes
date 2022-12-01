import sys, os, re

rpc_func_text = """
{RET} {GLOBAL_NAME}({PARAMS}) {{
  u32 target_node = {TARGET_NODE};
  if (target_node == rpc_->node_id_) {{
    LocalAddBlobIdToVBucket(
      {PASS_PARAMS});
  }} else {{
    RpcCall<bool>(rpc, target_node, "{GLOBAL_NAME}",
      {PASS_PARAMS});
  }}
}}
"""

rpc_lambda_text = """
auto {LAMBDA_NAME} = [{CONTEXT}](const request &req, {PARAMS}) {{
  auto result = {CONTEXT}->{LOCAL_NAME}({PASS_PARAMS});
  req.respond(result);
}};
rpc_server->define("{GLOBAL_NAME}", rpc_get_buffers);
"""

class Api:
    def __init__(self, api_str, target_node, context):
        self.api_str = api_str
        self.target_node = target_node
        self.context = context
        self.name = None  # The name of the API
        self.global_name = None  # Name of API without "Local"
        self.lambda_name = None # Name of lambda caller of API (rpc_{name})
        self.ret = None  # Return value of the API
        self.var_defs = None  # The variables in the API
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
            self.global_name = self.name.replace("Local")
            self.lambda_name = f"Rpc{self.global_name}"
            self.ret = " ".join(proto[:-1])
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

class RpcGenerator:
    def __init__(self):
        self.path = None
        self.rpcs = {}

    def set_file(self, path, macro):
        self.path = path
        self.macro = macro

    def add(self, prototype, target_node, context):
        if self.path not in self.rpcs:
            self.rpcs[self.path] = {}
        if self.macro not in self.rpcs[self.path]:
            self.rpcs[self.path][self.macro] = []
        self.rpcs[self.path][self.macro].append(
            Api(prototype, target_node, context))

    def generate(self):
        for path, macro_rpcs in self.rpcs.items():
            class_lines = []
            with open(path) as fp:
                class_lines = fp.read().splitlines()

            rpc_lambdas = []
            for macro, rpcs in macro_rpcs.items():
                global_rpc_funcs = []
                i = self.find_macro(macro, class_lines)
                space = self.get_macro_space(i, class_lines)

                for rpc in rpcs:
                    self.create_global_rpc_func(rpc, space, global_rpc_funcs)
                    self.create_rpc_lambda(rpc, space, rpc_lambdas)

                class_lines = class_lines[:i+1] + \
                              global_rpc_funcs + \
                              class_lines[i:]
                class_text = "\n".join(class_lines)

    def find_macro(self, macro, lines):
        for i, line in enumerate(lines):
            if macro in line:
                return i

    def get_macro_space(self, i, lines):
        return len(lines[i]) - len(lines[i].lstrip())

    def add_space(self, space, text):
        lines = text.splitlines()
        for j, line in enumerate(lines):
            lines[j] = f"{space}{line}"
        return "\n".join(lines)

    def create_global_rpc_func(self, rpc, space, lines):
        lines.append(rpc_func_text.format(
            RET=rpc.ret,
            GLOBAL_NAME=rpc.global_name,
            PARAMS=rpc.get_args(),
            PASS_PARAMS=rpc.pass_params(),
            TARGET_NODE=rpc.target_node
        ))
        self.add_space(space, lines[-1])

    def create_rpc_lambda(self, rpc, space, lines):
        lines.append(rpc_lambda_text.format(
            LAMBDA_NAME=rpc.lambda_name,
            CONTEXT=rpc.context,
            PARAMS=rpc.get_params(),
            PASS_PARAMS=rpc.pass_params(),
            LOCAL_NAME=rpc.name
        ))
        self.add_space(space, lines[-1])

