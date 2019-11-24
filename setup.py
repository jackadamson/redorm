import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fr:
    requirements = [
        l.strip() for l in fr.read().split("\n") if len(l) > 0 and "#" not in l
    ]

setuptools.setup(
    name="redorm",
    version="0.2.0",
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
