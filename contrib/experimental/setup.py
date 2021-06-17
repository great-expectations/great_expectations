import os

from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), "./PACKAGE_VERSION")) as versionfile:
    version = versionfile.read().strip()

long_description = "Great Expectations community contributions package. (See https://github.com/great-expectations/great_expectations for full description)."

config = {
    "description": "Always know what to expect from your data.",
    "author": "The Great Expectations Team",
    "url": "https://github.com/great-expectations/great_expectations",
    "author_email": "team@greatexpectations.io",
    "version": version,
    "install_requires": ["great_expectations>=0.13.0"],
    "packages": find_packages(),
    "name": "great_expectations_experimental",
    "long_description": long_description,
    "license": "Apache-2.0",
    "keywords": "data science testing pipeline data quality dataquality validation datavalidation",
    "include_package_data": True,
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Topic :: Software Development :: Testing",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
}

setup(**config)
