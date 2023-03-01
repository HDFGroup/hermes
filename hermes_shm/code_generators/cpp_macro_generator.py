from code_generators.c_macro.macro_generator import CppMacroGenerator
import pathlib, os

PROJECT_ROOT=pathlib.Path(__file__).parent.parent.resolve()

DATA_STRUCTURE_TEMPLATES='include/hermes_shm/data_structures/internal/template'
DATA_STRUCTURE_INTERNAL='include/hermes_shm/data_structures/internal'

CppMacroGenerator().generate(
    os.path.join(PROJECT_ROOT,
                 f"{DATA_STRUCTURE_TEMPLATES}/shm_container_base_template.h"),
    os.path.join(PROJECT_ROOT,
                f"{DATA_STRUCTURE_INTERNAL}/shm_container_macro.h"),
    "SHM_CONTAINER_TEMPLATE",
    ["CLASS_NAME", "TYPED_CLASS", "TYPED_HEADER"],
    ["TYPE_UNWRAP", "TYPE_UNWRAP", "TYPE_UNWRAP"],
    "HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_MACRO_H_")
