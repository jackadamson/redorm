import setuptools
import re

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fr:
    requirements = [
        l.strip() for l in fr.read().split("\n") if len(l) > 0 and "#" not in l
    ]

with open("redorm/__init__.py", "r") as f:
    version_match = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.MULTILINE
    )
    if version_match is None:
        raise ValueError("Version not found in aiplayerground/__init__.py")
    version = version_match.group(1)

setuptools.setup(
    name="redorm",
    version=version,
    author="Jack Adamson",
    author_email="jack@mrfluffybunny.com",
    description="A simple redis ORM",
    install_requires=requirements,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jackadamson/redorm",
    packages=setuptools.find_packages(exclude=["tests"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires=">=3.6",
)
