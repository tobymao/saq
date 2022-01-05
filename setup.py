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
    description="Distributed Python job queue with asyncio and redis",
    long_description=open("README.md", encoding="utf8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/tobymao/saq",
    author="Toby Mao",
    author_email="toby.mao@gmail.com",
    license="MIT",
    packages=["saq"],
    include_package_data=True,
    entry_points="""
        [console_scripts]
        saq=saq.__main__:main
    """,
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
        "Development Status :: 3 - Alpha",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
        "Operating System :: OS Independent",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
    ],
)
