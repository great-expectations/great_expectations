from typing import List

import setuptools


def get_requirements() -> List[str]:
    with open("requirements.txt") as f:
        requirements = f.read().splitlines()
    return requirements


setuptools.setup(
    name="great_expectations_semantic_types_expectations",
    version="0.1.0",
    install_requires=get_requirements(),
)
