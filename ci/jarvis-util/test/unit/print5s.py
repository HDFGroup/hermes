"""
NOTE: this is a helper utility for test_local_exec
"""

import time
import sys


for i in range(5):
    sys.stdout.write(f"COUT: {i}\n")
    sys.stderr.write(f"CERR: {i}\n")
    time.sleep(1)