import pprint
import sys
import clang.cindex
from clang.cindex import CursorKind, TokenKind
import os, sys
import json
from enum import Enum
import inspect


class OpType:
    COMMENT = "COMMENT"
    METHOD = "METHOD"


def parse_compiler_includes(COMPILER_INCLUDES):
    with open(COMPILER_INCLUDES) as fp:
        lines = fp.read().splitlines()
    cmd = " ".join(lines[4:])
    toks = cmd.split()
    args = []
    for i, tok in enumerate(toks):
        if "-I" in tok:
            tok = tok.replace('"', '')
            args.append(tok)
        elif "-internal-isystem" in tok:
            tok = toks[i+1]
            tok = tok.replace('"', '')
            args.append(f"-I{tok}")
    return args


def parse_compile_commands(COMPILER_COMMANDS, CMAKE_CURRENT_BINARY_DIR,
                           includes):
    sources = []
    with open(COMPILER_COMMANDS) as fp:
        compile_commands = json.load(fp)
    for cmd_info in compile_commands:
        dir = cmd_info['directory']
        if not os.path.samefile(dir, CMAKE_CURRENT_BINARY_DIR):
            continue
        arg_str = cmd_info['command']
        toks = arg_str.split()[1:]
        args = []
        for i, tok in enumerate(toks):
            if "-I" in tok:
                tok = tok.replace('"', '')
                args.append(tok)
            elif "-isystem" in tok:
                tok = toks[i+1]
                tok = tok.replace('"', '')
                args.append(f"-I{tok}")
            elif "-internal-isystem" in tok:
                tok = toks[i+1]
                tok = tok.replace('"', '')
                args.append(f"-I{tok}")
            elif "-D" in tok:
                tok = tok.replace('"', '')
                args.append(tok)
        sources.append({
            'source': cmd_info['file'],
            'args': args + includes
        })
    return sources


def find_comments(cursor, info=None):
    if info is None:
        info = {}
    for x in cursor.get_tokens():
        if x.kind == TokenKind.COMMENT:
            path = x.start.file
            if path not in info:
                info[path] = []
            info[path].append((OpType.COMMENT,
                               x.start.line,
                               x.start.column,
                               x.spelling))
    children = list(cursor.get_children())
    for child in children:
        find_comments(child)
    return info


def find_functions(cursor, info=None, cur_ns=[]):
    if info is None:
        info = {}
    children = list(cursor.get_children())
    ns_types = {
        CursorKind.NAMESPACE,
        CursorKind.STRUCT_DECL,
        CursorKind.UNION_DECL,
        CursorKind.CLASS_DECL
    }
    function_types = {
        CursorKind.FUNCTION_DECL,
        CursorKind.CXX_METHOD
    }
    for x in children:
        if x.kind in ns_types:
            cur_ns += [x.displayname]
            find_functions(x, info, cur_ns)
        elif x.kind in function_types:
            path = x.start.file
            exit()
            if path not in info:
                info[path] = []
            info[path].append((OpType.METHOD,
                               x.start.line,
                               x.start.column,
                               cur_ns,
                               x.displayname))
        find_functions(x, info)
    return info


### MAIN START
CMAKE_SOURCE_DIR = sys.argv[1]
CMAKE_BINARY_DIR = sys.argv[2]
COMPILER_INCLUDES = sys.argv[3]
COMPILER_COMMANDS = sys.argv[4]
CMAKE_CURRENT_SOURCE_DIR = os.path.join(CMAKE_SOURCE_DIR, 'src')
CMAKE_CURRENT_BINARY_DIR = os.path.join(CMAKE_BINARY_DIR, 'src')

# Load the CLANG commands from YAML
includes = parse_compiler_includes(COMPILER_INCLUDES)
sources = parse_compile_commands(COMPILER_COMMANDS, CMAKE_CURRENT_BINARY_DIR, includes)

# Call Clang
for source in sources:
    print(f"Parsing {source['source']}")
    extra_args = ["-fparse-all-comments"]
    index = clang.cindex.Index.create()
    tu = index.parse(source['source'], args=extra_args + source['args'])
    diagnostics = list(tu.diagnostics)
    if len(diagnostics) > 0:
        print(f'There were {len(diagnostics)} parse errors')
        for err in diagnostics:
            if err.severity > 3:
                pprint.pprint(err)
                exit()
    #info = find_comments(tu.cursor)
    info = find_functions(tu.cursor)
    pprint.pprint(info)
    exit()
