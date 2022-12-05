
"""
Create the .h and .cc files for defining Singletons
USAGE:
    cd code_generators/bin
    python3 singleton.py
"""

import re
from code_generators.singleton.generator import SingletonGenerator
from code_generators.util.paths import HERMES_ROOT

gen = SingletonGenerator("SRC", "\"singleton.h\"")
gen.add("hermes::api", "Hermes", "hermes.h")
gen.generate(f"{HERMES_ROOT}/src/singleton.cc",
             f"{HERMES_ROOT}/src/singleton_macros.h")