import uuid
from unittest import mock
from unittest.mock import Mock  # noqa: TID251 # FIXME CoP

import pytest
from ruamel.yaml.representer import RepresenterError

from great_expectations.data_context.types.base import (
    DataContextConfig,
    ExecutionEngineConfigSchema,
    object_to_yaml_str,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "connect_args",
    [
        {"connection_factory": Mock()},
        {"ssl_verify_cert": False},
        {"timeout": 30},
        {
            "ssl": {
                "ssl_ca": "ca.pem",
                "ssl_cert": "client-cert.pem",
                "ssl_key": "client-key.pem",
            }
        },
    ],
)
def test_execution_engine_config_conect_args(connect_args):
    """
    this is part of a test-driven fix for: https://github.com/great-expectations/great_expectations/issues/6226
    connect_args examples are here: https://docs.sqlalchemy.org/en/20/core/engines.html#use-the-connect-args-dictionary-parameter
    """
    cfg = ExecutionEngineConfigSchema().load(
        {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": "sqlite://",
            "connect_args": connect_args,
        }
    )

    assert cfg.connect_args == connect_args


@pytest.mark.unit
def test_object_to_yaml_str_recovers_from_a_failed_dump():
    """A dump that raises must not leave later dumps broken.

    Serialization state is per-call; a value YAML cannot represent should fail that
    call alone, not every subsequent one in the process.
    """
    unrepresentable = {"context_id": uuid.uuid4()}

    with pytest.raises(RepresenterError):
        object_to_yaml_str(unrepresentable)

    assert object_to_yaml_str({"a": 1}) == "a: 1\n"


@pytest.mark.unit
def test_to_yaml_recovers_from_a_failed_dump(tmp_path):
    """The same guarantee for file-backed dumps, and across the two entry points."""
    config = DataContextConfig(stores={}, expectations_store_name="expectations_store")
    outfile = tmp_path / "out.yml"

    with mock.patch.object(
        DataContextConfig,
        "commented_map",
        new_callable=mock.PropertyMock,
        return_value={"context_id": uuid.uuid4()},
    ):
        with pytest.raises(RepresenterError), outfile.open("w") as f:
            config.to_yaml(f)

    # A later dump through the *other* entry point must still work.
    assert object_to_yaml_str({"a": 1}) == "a: 1\n"
