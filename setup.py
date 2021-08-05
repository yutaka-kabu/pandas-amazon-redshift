#!/usr/bin/env python
# -*- coding: utf-8 -*-

import versioneer
from setuptools import find_packages, setup

NAME = "pandas-amazon-redshift"


# versioning
cmdclass = versioneer.get_cmdclass()


def readme():
    with open("README.rst") as f:
        return f.read()


INSTALL_REQUIRES = [
    "setuptools",
    "pandas>=1.3.0",
    "boto3>=1.17.100",
    "botocore>=1.20.100",
]

setup(
    name=NAME,
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Pandas interface to Amazon Redshift",
    long_description=readme(),
    license="BSD License",
    author="Yutaka Kabutoya",
    author_email="y.kabutoya@gmail.com",
    url="https://github.com/yutaka-kabu/pandas-redshift",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering",
    ],
    keywords="data",
    install_requires=INSTALL_REQUIRES,
    python_requires=">=3.7.1",
    packages=find_packages(exclude=["boto3_mock*", "tests*"]),
    test_suite="tests",
)
