# Adapter Generator

This library generates an interceptor "API" class used to store the original implementations
of a library for an LD_PRELOAD interceptor to use.

## Usage

To create the interceptor for POSIX:
```bash
cd /path/to/adapter_generator
python3 posix.py
```

This will create the file adapter/posix/posix.h