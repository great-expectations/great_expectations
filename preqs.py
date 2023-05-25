import re

# TODO: remove this script before merge
text = """
boto3>=1.17.106
flask>=1.0.0
freezegun>=0.3.15
ipykernel<=6.17.1
mock-alchemy>=0.2.5
moto>=2.0.0,<3.0.0
nbconvert>=5
pyfakefs>=4.5.1
pytest>=6.2.0
pytest-benchmark>=3.4.1
pytest-cov>=2.8.1
pytest-icdiff>=0.6
pytest-mock>=3.8.2
pytest-order>=0.9.5
pytest-random-order>=1.0.4
pytest-timeout>=2.1.0
requirements-parser>=0.2.0
responses>=0.23.1 # requests mocking
snapshottest==0.6.0 # GX Cloud atomic renderer tests
sqlalchemy>=1.4.0,<2.0.0
"""

group_name = "lite"

tool_config = f"""[tool.poetry.group.{group_name}]
optional = false

[tool.poetry.group.{group_name}.dependencies]"""
print(tool_config)

reqs = text.lstrip().splitlines()
for req in reqs:
    pkg, operator, version = re.split(r"(?P<operator>>=|<=|==)", req, maxsplit=1)
    print(f'{pkg} = "{operator}{version}"')
