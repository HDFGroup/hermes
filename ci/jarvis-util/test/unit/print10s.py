import time
import sys


for i in range(10):
    sys.stdout.write(f"COUT: {i}\n")
    sys.stderr.write(f"CERR: {i}\n")
    time.sleep(1)