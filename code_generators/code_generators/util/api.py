import sys, os, re

class Api:
    def __init__(self, api_str):
        self.api_str = api_str
        self.name = None  # The name of the API
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