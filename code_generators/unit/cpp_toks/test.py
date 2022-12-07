from code_generators.decorator.api import Api
from code_generators.rpc.generator import RpcGenerator

text = """
WRAP RPC std::vector<int>& Localf5(int a,
                              std::vector<int> b,
                              std::string x = "/*>*/",
                              Ctx ctx = Ctx());
"""

text = """
RPC int f2();
"""

gen = RpcGenerator()
api = Api(text, [gen])

print(api.name)
print(api.ret)
print(api.params)
print(api.get_args())
print(api.pass_args())