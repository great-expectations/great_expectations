from __future__ import annotations

import pydantic
import pytest
from pytest import MonkeyPatch
from typing import Union

from great_expectations.core.config_provider import (
    _ConfigurationProvider,
    _EnvironmentConfigurationProvider,
)
from great_expectations.datasource.fluent.config_str import ConfigStr, SecretStr
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel


def test_config_provider_substitution(
    monkeypatch: MonkeyPatch,
):
    config_provider = _ConfigurationProvider()
    config_provider.register_provider(_EnvironmentConfigurationProvider())

    monkeypatch.setenv("MY_CONFIG", "bar")

    my_dict = {"my_key": r"foo${MY_CONFIG}", "another_key": r"${MY_CONFIG}"}
    subbed = config_provider.substitute_config(my_dict)

    assert subbed == {"my_key": "foobar", "another_key": "bar"}
    assert subbed != my_dict


def test_config_str_validation():
    class MyClass(FluentBaseModel):
        normal_field: str
        secret_field: SecretStr
        config_field: ConfigStr

    with pytest.raises(pydantic.ValidationError, match="ConfigStr"):
        m = MyClass(
            normal_field="normal", secret_field="secret", config_field="invalid config"
        )
        print(m)


@pytest.mark.parametrize(
    ["input_value", "expected"],
    [
        (r"${VALID_CONFIG_STR}", r"${VALID_CONFIG_STR}"),
        ("postgres://userName@hostname", "postgres://userName@hostname"),
    ],
)
def test_as_union_file_type(input_value, expected):
    class MyClass(FluentBaseModel):
        my_field: Union[ConfigStr, pydantic.networks.PostgresDsn]

    m = MyClass(my_field=input_value)
    print(m)

    assert str(m.my_field) == expected


def test_config_substitution():
    pass


def test_serialization():
    pass


def test_nested_serialization():
    pass


class TestSecretMasking:
    def test_validation_error(self):
        pass

    def test_repr(self):
        pass

    def test_str(self):
        pass

    def test_serialization(self):
        pass
