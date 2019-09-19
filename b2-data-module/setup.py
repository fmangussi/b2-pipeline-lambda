from setuptools import setup, find_packages
import os
with open('datamodule/requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="datamodule",
    version="0.1",
    packages=find_packages(),
    install_requires=required
)