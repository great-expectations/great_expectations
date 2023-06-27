import pytest

import great_expectations.exceptions as gx_exceptions


def read_config_from_file(config_filename):
    with open(config_filename) as f_:
        config = f_.read()

    return config


def test_add_store_immediately_adds_to_config(empty_data_context):
    context = empty_data_context
    config_filename = context.root_directory + "/great_expectations.yml"

    assert "my_new_store" not in read_config_from_file(config_filename)
    context.add_store(
        "my_new_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        },
    )
    assert "my_new_store" in read_config_from_file(config_filename)


@pytest.mark.integration
def test_add_data_docs_site_immediately_adds_to_config(empty_data_context):
    context = empty_data_context
    config_filename = context.root_directory + "/great_expectations.yml"

    new_site_name = "my_new_site"
    new_site_config = {
        "class_name": "SiteBuilder",
        "module_name": "great_expectations.render.renderer.site_builder",
        "store_backend": {
            "module_name": "great_expectations.data_context.store.tuple_store_backend",
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": context.root_directory + "/my_new_site/",
        },
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    }

    assert new_site_name not in read_config_from_file(config_filename)
    context.add_data_docs_site(
        new_site_name,
        new_site_config,
    )
    assert new_site_name in read_config_from_file(config_filename)


def test_add_datasource(empty_data_context):
    context = empty_data_context
    config_filename = context.root_directory + "/great_expectations.yml"

    # Config can't be instantiated
    with pytest.raises(gx_exceptions.DatasourceInitializationError):
        context.add_datasource(
            "my_new_datasource",
            **{
                "some": "broken",
                "config": "yikes",
            },
        )
    assert "my_new_datasource" not in context.datasources
    assert "my_new_datasource" not in read_config_from_file(config_filename)

    # Config doesn't instantiate an Datasource
    with pytest.raises(gx_exceptions.DatasourceInitializationError):
        context.add_datasource(
            "my_new_datasource",
            **{
                "module_name": "great_expectations.data_context.store",
                "class_name": "ExpectationsStore",
            },
        )
    assert "my_new_datasource" not in context.datasources
    assert "my_new_datasource" not in read_config_from_file(config_filename)

    # Config successfully instantiates an Datasource
    context.add_datasource(
        "my_new_datasource",
        **{
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                "test_runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["run_id", "y", "m", "d"],
                }
            },
        },
    )
    assert "my_new_datasource" in context.datasources
    assert "my_new_datasource" in read_config_from_file(config_filename)
