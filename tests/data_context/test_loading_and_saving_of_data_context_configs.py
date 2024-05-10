import pytest

pytestmarks = pytest.mark.filesystem


def read_config_from_file(config_filename):
    with open(config_filename) as f_:
        config = f_.read()

    return config


@pytest.mark.filesystem
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
