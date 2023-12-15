from typing import List

import setuptools
from setuptools import find_packages


def get_requirements() -> List[str]:
    with open("requirements.txt") as f:
        requirements = f.read().splitlines()
    return requirements


setuptools.setup(
    name="capitalone-dataprofiler-expectations",
    version="0.1.0",
    install_requires=get_requirements(),
    packages=find_packages(exclude=["assets", "tests"]),
)
