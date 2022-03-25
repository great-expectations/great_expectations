import re
from glob import glob

from setuptools import find_packages, setup

import versioneer


def get_extras_require():
    results = {}
    extra_keys = {
        "aws_secrets": "boto",
        "azure_secrets": "azure",
        "gcp": "bigquery",
        "s3": "boto",
    }
    ignore_keys = ("contrib", "sqlalchemy", "test")
    rx = re.compile(r"requirements-dev-(.*).txt")
    for fname in sorted(glob("requirements-dev-*.txt")):
        key = rx.match(fname).group(1)
        if key in ignore_keys:
            continue
        with open(fname) as f:
            results[key] = f.read().splitlines()

    lite = results.pop("lite")
    results["boto"] = [req for req in lite if req.startswith("boto")]
    results["sqlalchemy"] = [req for req in lite if req.startswith("sqlalchemy")]

    for new_key, existing_key in extra_keys.items():
        results[new_key] = results[existing_key]

    results.pop("boto")
    return results


# Parse requirements.txt
with open("requirements.txt") as f:
    required = f.read().splitlines()

# try:
#    import pypandoc
#    long_description = pypandoc.convert_file('README.md', 'rst')
# except (IOError, ImportError):
long_description = "Always know what to expect from your data. (See https://github.com/great-expectations/great_expectations for full description)."

config = {
    "description": "Always know what to expect from your data.",
    "author": "The Great Expectations Team",
    "url": "https://github.com/great-expectations/great_expectations",
    "author_email": "team@greatexpectations.io",
    "version": versioneer.get_version(),
    "cmdclass": versioneer.get_cmdclass(),
    "install_requires": required,
    "extras_require": get_extras_require(),
    "packages": find_packages(exclude=["contrib*", "docs*", "tests*", "examples*"]),
    "entry_points": {
        "console_scripts": ["great_expectations=great_expectations.cli:main"]
    },
    "name": "great_expectations",
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
        "Programming Language :: Python :: 3.9",
    ],
}

setup(**config)
