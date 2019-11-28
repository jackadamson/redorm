#!/bin/env python3
import re

with open("redorm/_version.py", "r") as f:
    version_match = re.search(
        r'^version \s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.MULTILINE
    )
    if version_match is None:
        raise ValueError("Version not found in redorm/_version.py")
    version = version_match.group(1)
print(f"##[set-output name=version;]v{version}")
