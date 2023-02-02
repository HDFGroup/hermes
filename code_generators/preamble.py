"""
Prepends the preabmle to all files in the repo

USAGE:
    python3 scripts/preamble.py ${HERMES_ROOT}
"""

import sys,os,re
import pathlib

preamble = """
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
"""

def PrependPreamble(path):
    text = ""
    with open(path, 'r') as fp:
        text = fp.read()
        if "Copyright (C) 2022  SCS Lab <scslab@iit.edu>" in text:
            text = text.replace(preamble, "")
        text = text.strip()
        text = re.sub("//\n// Created by [^\n]*\n//\n", "", text)
        text = preamble.strip() + text + "\n"

    with open(path, 'w') as fp:
        fp.write(text)


def LocateCppFiles(root):
    for entry in os.listdir(root):
        full_path = os.path.join(root, entry)
        if os.path.isdir(full_path):
            LocateCppFiles(full_path)
        elif os.path.isfile(full_path):
            file_ext = pathlib.Path(full_path).suffix
            if file_ext == '.cc' or file_ext == '.h':
                PrependPreamble(full_path)


root = sys.argv[1]
LocateCppFiles(root)