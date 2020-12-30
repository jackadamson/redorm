#!/bin/env python3
import redorm

version = redorm.__version__

print(f"##[set-output name=version;]v{version}")
