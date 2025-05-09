#!/usr/bin/env python
# -*- coding: utf-8 -*-
# To generate new tag, update tag name in __version__.py and run python setup.py upload
# To remove exist tag, run git tag -d <tag_name>
# Note: To use the 'upload' functionality of this file, you must:
#   $ pip install twine

import io
import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

# Package meta-data.
NAME = "data-engineering-dag-factory"
PKG_NAME = "dagfactory"
DESCRIPTION = "Dynamically build Airflow DAGs from YAML files"
URL = "https://github.com/pelotoncycle/dag-factory"
EMAIL = "data-engineering@onepeloton.com"
AUTHOR = "Data Engineering Team"
REQUIRES_PYTHON = ">=3.7.0"
VERSION = None

here = os.path.abspath(os.path.dirname(__file__))

REQUIRED = ["apache-airflow[http,kubernetes]>=1.10.0", "pyyaml", "packaging", "click"]
DEV_REQUIRED = ["black", "pytest", "pylint", "pytest-cov", "tox"]

try:
    with io.open(os.path.join(here, "README.md"), encoding="utf-8") as f:
        long_description = "\n" + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    with open(os.path.join(here, PKG_NAME, "__version__.py")) as f:
        exec(f.read(), about)
else:
    about["__version__"] = VERSION


class UploadCommand(Command):
    """Support setup.py upload."""

    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
        except OSError:
            pass

        self.status("Building Source and Wheel (universal) distribution…")
        os.system("{0} setup.py sdist bdist_wheel --universal".format(sys.executable))

        # self.status("Uploading the package to PyPI via Twine…")
        # os.system("twine upload dist/*")
        #
        self.status("Pushing git tags…")
        os.system("git tag v{0}".format(about["__version__"]))
        os.system("git push --tags")

        sys.exit()


setup(
    name=NAME,
    version=about["__version__"],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=("tests",)),
    install_requires=REQUIRED,
    extras_require={"dev": DEV_REQUIRED},
    include_package_data=True,
    license="MIT",
    keywords="airflow",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    entry_points={
        'console_scripts': [
            'dagfactory = dagfactory.scripts.cli:cli',
        ],
    },
    # $ setup.py publish support.
    cmdclass={"upload": UploadCommand},
)
