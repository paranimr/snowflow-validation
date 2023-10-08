"""
Description: Setup script to create the egg file (to be used within all Pyshell jobs)

To create, make sure you have are running Python 2.7 and run the following command:

    python setup.py bdist_egg

"""

import os
from setuptools import setup, find_packages

setup(
    name='MLFLib',
    classifiers = ["Programming Language :: Python :: 3.8"],
    author='Parani',
    version='1.0',
    packages=find_packages(),
    # install_requires=install_requires,
    include_package_data=True
)

