import os
from collections import OrderedDict

import pytest
from ruamel.yaml import YAML, YAMLError

import great_expectations as ge
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigSchema,
)
from great_expectations.data_context.util import (
    file_relative_path,
    substitute_config_variable,
)
from great_expectations.exceptions import InvalidConfigError, MissingConfigVariableError
from tests.data_context.conftest import create_data_context_files

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

dataContextConfigSchema = DataContextConfigSchema()


def test_substituted_config_variables_not_written_to_file(tmp_path_factory):
    # this test uses a great_expectations.yml with almost all values replaced
    # with substitution variables

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_exhaustive_variables.yml",
        config_variables_fixture_filename="config_variables_exhaustive.yml",
    )

    # load ge config fixture for expected
    path_to_yml = (
        "../test_fixtures/great_expectations_basic_with_exhaustive_variables.yml"
    )
    path_to_yml = file_relative_path(__file__, path_to_yml)
    with open(path_to_yml) as data:
        config_commented_map_from_yaml = yaml.load(data)
    expected_config = DataContextConfig.from_commented_map(
        config_commented_map_from_yaml
    )
    expected_config_commented_map = dataContextConfigSchema.dump(expected_config)
    expected_config_commented_map.pop("anonymous_usage_statistics")

    # instantiate data_context twice to go through cycle of loading config from file then saving
    context = ge.data_context.DataContext(context_path)
    context._save_project_config()
    context_config_commented_map = dataContextConfigSchema.dump(
        ge.data_context.DataContext(context_path)._project_config
    )
    context_config_commented_map.pop("anonymous_usage_statistics")

    assert context_config_commented_map == expected_config_commented_map
