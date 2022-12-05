
"""
Create the .h and .cc files for defining Singletons
USAGE:
    cd hermes/code_generators/singleton
    python3 generate.py
"""

import re

def ToCamelCase(string):
    if string is None:
        return
    words = re.sub(r"(_|-)+", " ", string).split()
    words = [word.capitalize() for word in words]
    return "".join(words)

def ToSnakeCase(string):
    if string is None:
        return
    string = re.sub('(\.|-)+', '_', string)
    words = re.split('([A-Z][^A-Z]*)', string)
    words = [word for word in words if len(word)]
    string = "_".join(words)
    return string.lower()

class SingletonDefinition:
    def __init__(self, namespace, class_name, include):
        snake = ToSnakeCase(class_name).upper()
        if snake == "HERMES":
            self.macro_name = f"HERMES"
        else:
            self.macro_name = f"HERMES_{snake}"
        self.type_name = f"{self.macro_name}_T"
        if namespace is not None:
            self.class_name = f"{namespace}::{class_name}"
        else:
            self.class_name = class_name
        self.include = include

class SingletonGenerator:
    def __init__(self, guard_name, singleton_include):
        self.singleton_include = singleton_include
        self.guard_name = guard_name
        self.defs = []

    def add(self, namespace, class_name, include):
        self.defs.append(SingletonDefinition(
            namespace, class_name, include
        ))

    def generate(self, cc_file, h_file):
        self._generate_cc(cc_file)
        self._generate_h(h_file)

    def _generate_cc(self, path):
        lines = []
        lines.append(f"#include {self.singleton_include}")
        lines.append("")
        for defn in self.defs:
            lines.append(f"#include <{defn.include}>")
            lines.append(f"template<> std::unique_ptr<{defn.class_name}> hermes::Singleton<{defn.class_name}>::obj_ = nullptr;")
        self._save_lines(lines, path)

    def _generate_h(self, path):
        lines = []
        lines.append(f"#ifndef HERMES_SINGLETON_{self.guard_name}_MACROS_H")
        lines.append(f"#define HERMES_SINGLETON_{self.guard_name}_MACROS_H")
        lines.append("")
        lines.append(f"#include {self.singleton_include}")
        lines.append("")
        for defn in self.defs:
            lines.append(f"#define {defn.macro_name} hermes::Singleton<{defn.class_name}>::GetInstance()")
            lines.append(f"#define {defn.type_name} {defn.class_name}*")
            lines.append("")
        lines.append(f"#endif  // HERMES_SINGLETON_{self.guard_name}_MACROS_H")
        self._save_lines(lines, path)

    def _save_lines(self, lines, path):
        text = "\n".join(lines) + "\n"
        if path is None:
            print(text)
            return
        with open(path, 'w') as fp:
            fp.write(text)

gen = SingletonGenerator("SRC", "\"singleton.h\"")
gen.add("hermes::api", "Hermes", "hermes.h")
gen.generate("../../src/singleton.cc",
             "../../src/singleton_macros.h")