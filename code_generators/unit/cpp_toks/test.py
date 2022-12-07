from code_generators.decorator.api import Api
from code_generators.rpc.generator import RpcGenerator

text = """
WRAP RPC std::vector<int>& Localf5(int a,
                              std::vector<int> b,
                              std::string x = "/*>*/",
                              Ctx ctx = Ctx());
"""

text2 = """
RPC int f2();
"""

gen = RpcGenerator()
api = Api(None, None, text, [gen])

print(api)