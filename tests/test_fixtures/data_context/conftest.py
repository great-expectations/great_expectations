import pytest

pytest.fixture()
def taxicab_context():
    return DataContext(config=file_relative_path("./configs/great_expectations_taxicab_context.yml"))
