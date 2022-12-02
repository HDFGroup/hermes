import sys, os, re

rpc_func_text = """
{RET} {GLOBAL_NAME}({PARAMS}) {{
  u32 target_node = {TARGET_NODE};
  if (target_node == rpc_->node_id_) {{
    LocalAddBlobIdToVBucket(
      {PASS_PARAMS});
  }} else {{
    rpc_->Call<bool>(rpc, target_node, "{GLOBAL_NAME}",
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
    def __init__(self, api_str):
        self.api_str = api_str
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
        toks = self._clean(re.split("[()]|;", api_str))
        proto, args = toks[0], toks[1]

        try:
            proto = self._clean(re.split("[ ]|(\*+)", proto))
            self.name = proto[-1]
            self.global_name = self.name.replace("Local", "")
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
        self.macro = None
        self.context = None
        self.rpcs = {}

    def set_file(self, path, class_name, context):
        self.path = path
        self.class_name = class_name
        self.context = context

    def add(self, local_rpc_name, target_node):
        if self.path not in self.rpcs:
            self.rpcs[self.path] = {}
        if self.class_name not in self.rpcs[self.path]:
            self.rpcs[self.path][self.class_name] = []
        self.rpcs[self.path][self.class_name].append(
            (local_rpc_name, target_node, self.context))

    def generate(self):
        rpc_lambdas = []
        for path, class_rpcs in self.rpcs.items():
            self.generate_class_file(path, class_rpcs, rpc_lambdas)
        self.generate_rpc_file(rpc_lambdas)

    def generate_class_file(self, path, class_rpcs, rpc_lambdas):
        with open(path) as fp:
            class_lines = fp.read().splitlines()
        rpc_map = self.get_rpcs_from_class(class_lines)
        for class_name, rpcs in class_rpcs.items():
            gen_start = rpc_map[class_name]['gen_start']
            gen_end = rpc_map[class_name]['gen_end']
            space = self.get_macro_space(class_lines[gen_start])
            global_rpc_funcs = []
            for rpc in rpcs:
                local_rpc_name = rpc[0]
                target_node = rpc[1]
                context = rpc[2]
                local_rpc_api = rpc_map[class_name]['apis'][local_rpc_name]
                self.create_global_rpc_func(local_rpc_api, target_node,
                                            space, global_rpc_funcs)
                self.create_rpc_lambda(local_rpc_api, context,
                                       space, rpc_lambdas)

            class_lines = class_lines[:gen_start+1] + class_lines[gen_end:]
            class_lines = class_lines[:gen_start + 1] + \
                          global_rpc_funcs + \
                          class_lines[gen_start + 1:]

        with open("tmp", 'w') as fp:
            fp.write("\n".join(class_lines))

    def generate_rpc_file(self, rpc_lambdas):
        path = "../rpc_thallium.cc"
        with open(path) as fp:
            rpc_lines = fp.read().splitlines()
        gen_start = self.find_macro("RPC_AUTOGEN_START", rpc_lines)
        gen_end = self.find_macro("RPC_AUTOGEN_END", rpc_lines)
        rpc_lines = rpc_lines[:gen_start+1] + rpc_lines[gen_end:]
        rpc_lines = rpc_lines[:gen_start+1] + \
                    rpc_lambdas + \
                    rpc_lines[gen_start+1:]
        with open("tmp2", 'w') as fp:
            fp.write("\n".join(rpc_lines))

    def get_rpcs_from_class(self, class_lines):
        cur_class = None
        rpc_map = {}
        for i, line in enumerate(class_lines):
            if "class" == line.strip()[0:5]:
                cur_class = self.get_class_name(line)
                rpc_map[cur_class] = {'apis': {},
                                      'gen_start': None,
                                      'gen_end': None}
            elif "RPC_AUTOGEN_START" in line:
                rpc_map[cur_class]['gen_start'] = i
            elif "RPC_AUTOGEN_END" in line:
                rpc_map[cur_class]['gen_end'] = i
            elif "RPC" == line.strip()[0:3]:
                text_proto = self.get_rpc_prototype(class_lines[i:])
                api = Api(text_proto)
                local_rpc_name = api.name
                rpc_map[cur_class]['apis'][local_rpc_name] = api
        return rpc_map

    def get_class_name(self, line):
        toks = re.split("[\:\*+ ]", line)
        toks = [tok.strip() for tok in toks if tok is not None and len(tok)]
        class_name = toks[1]
        return class_name

    def get_rpc_prototype(self, lines):
        proto_toks = []
        for line in lines:
            proto_toks.append(line.strip())
            if ';' in line:
                break
        proto = " ".join(proto_toks)
        return proto

    def find_macro(self, macro, lines):
        for i, line in enumerate(lines):
            if macro in line:
                return i

    def get_macro_space(self, line):
        space_len = len(line) - len(line.lstrip())
        return " " * space_len

    def add_space(self, space, text):
        lines = text.splitlines()
        for j, line in enumerate(lines):
            lines[j] = f"{space}{line}"
        return "\n".join(lines)

    def create_global_rpc_func(self, rpc, target_node, space, lines):
        lines.append(rpc_func_text.format(
            RET=rpc.ret,
            GLOBAL_NAME=rpc.global_name,
            PARAMS=rpc.get_args(),
            PASS_PARAMS=rpc.pass_args(),
            TARGET_NODE=target_node
        ).strip())
        lines[-1] = self.add_space(space, lines[-1])

    def create_rpc_lambda(self, rpc, context, space, lines):
        lines.append(rpc_lambda_text.format(
            GLOBAL_NAME=rpc.global_name,
            LAMBDA_NAME=rpc.lambda_name,
            CONTEXT=context,
            PARAMS=rpc.get_args(),
            PASS_PARAMS=rpc.pass_args(),
            LOCAL_NAME=rpc.name
        ).strip())
        lines[-1] = self.add_space(space, lines[-1])

