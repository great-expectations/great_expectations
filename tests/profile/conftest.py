import os
import shutil

import pytest

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def titanic_data_context_modular_api(tmp_path_factory, monkeypatch):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "../data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./fixtures/great_expectations_titanic_0.13.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "../data/Titanic.csv"))
    )
    return ge.data_context.DataContext(context_path)
