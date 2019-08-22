import pytest
import json
import os

from ruamel.yaml import YAML
yaml = YAML()
yaml.default_flow_style = False

import great_expectations as ge

@pytest.fixture(scope="function")
def totally_empty_data_context(tmp_path_factory):
    #TODO: This is such a weird workaround for initializing a DataContext. See https://github.com/great-expectations/great_expectations/issues/617
    project_root_dir = str(tmp_path_factory.mktemp('totally_empty_data_context'))
    os.mkdir(os.path.join(project_root_dir, 'great_expectations'))

    config = {
        "plugins_directory": "plugins/",
        "expectations_directory": "expectations/",
        "evaluation_parameter_store_name": "not_a_real_store_name",
        "datasources": {},
        "stores": {},
        "data_docs": {
            "sites": {}
        }
    }
    with open(os.path.join(project_root_dir, "great_expectations/great_expectations.yml"), 'w') as config_file:
        yaml.dump(
            config,
            config_file
        )

    context = ge.data_context.DataContext(os.path.join(project_root_dir, "great_expectations"))
    # print(json.dumps(context._project_config, indent=2))
    return context


def test_create(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('path_001'))
    context = ge.data_context.DataContext.create(project_path)

    assert isinstance(context, ge.data_context.DataContext)


def test_init(tmp_path_factory):
    #TODO: Deprecating this for now. See https://github.com/great-expectations/great_expectations/issues/617

    # project_path = str(tmp_path_factory.mktemp('path_002'))
    # context = ge.data_context.DataContext(context_root_dir=project_path)

    # assert isinstance(context, ge.data_context.DataContext)
    pass

def test_add_store(totally_empty_data_context):
    assert len(totally_empty_data_context.stores.keys()) == 0

    totally_empty_data_context.add_store(
        "my_inmemory_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "InMemoryStore"
        }
    )
    assert "my_inmemory_store" in totally_empty_data_context.stores.keys()
    assert len(totally_empty_data_context.stores.keys()) == 1


def test_config_from_absolute_zero(totally_empty_data_context):

    assert len(totally_empty_data_context.stores.keys()) == 0

    totally_empty_data_context.add_store(
        "my_inmemory_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "InMemoryStore",
            "store_config": {
                "serialization_type": "json"
            },
        }
    )
    assert "my_inmemory_store" in totally_empty_data_context.stores.keys()
    assert len(totally_empty_data_context.stores.keys()) == 1


def test_config_with_default_yml(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('totally_empty_data_context'))
    context = ge.data_context.DataContext.create(project_path)

    print(context.stores.keys())
    assert len(context.stores.keys()) == 6
    assert set(context.stores.keys()) == set([
        'local_validation_result_store',
        'local_profiling_store',
        'local_workbench_site_store',
        'evaluation_parameter_store',
        'fixture_validation_results_store',
        'shared_team_site_store',
    ])
    assert "my_inmemory_store" not in context.stores.keys()


    context.add_store(
        "my_inmemory_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "InMemoryStore",
            "store_config": {
                "serialization_type": "json"
            },
        }
    )

    print(context.stores.keys())
    assert len(context.stores.keys()) == 7
    assert "my_inmemory_store" in context.stores.keys()
    