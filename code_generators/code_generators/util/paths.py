import os,sys
import pathlib

def GetHermesRoot():
    util_path = pathlib.Path(__file__).parent.resolve()
    code_generators_path = os.path.dirname(util_path)
    code_generators_path = os.path.dirname(code_generators_path)
    hermes_path = os.path.dirname(code_generators_path)
    return hermes_path

HERMES_ROOT=GetHermesRoot()