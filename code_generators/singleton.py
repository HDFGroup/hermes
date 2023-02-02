
"""
Create the .h and .cc files for defining Singletons
USAGE:
    cd code_generators/bin
    python3 singleton.py
"""

import re
from code_generators.singleton.generator import SingletonGenerator
from code_generators.util.paths import HERMES_ROOT

# Hermes singleton
gen = SingletonGenerator("SRC", "\"singleton.h\"")
gen.add("hermes::api", "Hermes", "<hermes.h>")
gen.generate(f"{HERMES_ROOT}/src/singleton.cc",
             f"{HERMES_ROOT}/src/singleton_macros.h")

# POSIX RealAPI singleton
gen = SingletonGenerator("ADAPTER", "\"singleton.h\"")
gen.add("hermes::adapter::fs", "PosixApi", "\"real_api.h\"")
gen.generate(f"{HERMES_ROOT}/adapter/posix/singleton.cc",
             f"{HERMES_ROOT}/adapter/posix/singleton_macros.h")