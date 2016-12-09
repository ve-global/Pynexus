import re
import os
import sys

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


if sys.version_info < (3, 3, 0):
    raise RuntimeError("Pynexus requires Python 3.3.0+")

with open(os.path.join(os.path.dirname(__file__), 'pynexus/__init__.py'), 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

setup(
    name="pynexus",
    version=version,
    author="Julien Brayere",
    author_email="julien.brayere@veinteractive.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "tqdm",
        "requests",
        "coloredlogs"
    ],
)
