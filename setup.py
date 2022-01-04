# pylint: disable=consider-using-with
from setuptools import setup


version = (
    open("saq/__init__.py", encoding="utf8")
    .read()
    .split("__version__ = ")[-1]
    .split("\n")[0]
    .strip("")
    .strip("'")
    .strip('"')
)

setup(
    name="saq",
    version=version,
    description="A simple asyncronous queue",
    long_description=open("README.md", encoding="utf8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/tobymao/saq",
    author="Toby Mao",
    author_email="toby.mao@gmail.com",
    license="MIT",
    packages=["saq"],
    install_requires=[
        "aioredis>=2.0",
    ],
    extras_require={
        "hiredis": ["aioredis[hiredis]>=2.0"],
        "web": ["aiohttp"],
        "dev": [
            "black",
            "pylint",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
