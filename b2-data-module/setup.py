from setuptools import setup, find_packages
import os
with open('requirements.txt') as f:
    required = [item for item in f.read().splitlines() if len(item) > 0 and item[0] != '-' ]

setup(
    name="datamodule",
    version="0.1",
    packages=find_packages(),
    install_requires=required
)