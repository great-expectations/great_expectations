from __future__ import annotations

from typing import Callable, List, Union

import pydantic
import pytest
from pytest import MonkeyPatch

from great_expectations.core.config_provider import (
    _ConfigurationProvider,
    _EnvironmentConfigurationProvider,
)
from great_expectations.datasource.fluent.config_str import ConfigStr, SecretStr
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel


@pytest.fixture
def env_config_provider() -> _ConfigurationProvider:
    config_provider = _ConfigurationProvider()
    config_provider.register_provider(_EnvironmentConfigurationProvider())
    return config_provider


def test_config_provider_substitution(
    monkeypatch: MonkeyPatch, env_config_provider: _ConfigurationProvider
):
    monkeypatch.setenv("MY_CONFIG", "bar")

    my_dict = {"my_key": r"foo${MY_CONFIG}", "another_key": r"${MY_CONFIG}"}
    subbed = env_config_provider.substitute_config(my_dict)

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


def test_config_substitution(
    monkeypatch: MonkeyPatch, env_config_provider: _ConfigurationProvider
):
    class MyClass(FluentBaseModel):
        normal_field: str
        secret_field: SecretStr
        config_field: ConfigStr

    monkeypatch.setenv("MY_ENV_VAR", "success")

    m = MyClass(
        normal_field="normal",
        secret_field="secret",  # type: ignore[arg-type]
        config_field=r"${MY_ENV_VAR}",  # type: ignore[arg-type]
    )
    assert m.config_field.get_config_value(env_config_provider) == "success"


def test_config_substitution_alternate(
    monkeypatch: MonkeyPatch, env_config_provider: _ConfigurationProvider
):
    """TODO: maybe don't do this"""

    class MyClass(FluentBaseModel):
        normal_field: str
        secret_field: SecretStr
        config_field: ConfigStr

    monkeypatch.setenv("MY_ENV_VAR", "success")

    m = MyClass(
        normal_field="normal",
        secret_field="secret",  # type: ignore[arg-type]
        config_field=r"${MY_ENV_VAR}",  # type: ignore[arg-type]
    )
    m.config_field.config_provider = env_config_provider
    assert m.config_field.get_config_value() == "success"


@pytest.mark.parametrize("method", ["yaml", "dict", "json"])
def test_serialization(monkeypatch: MonkeyPatch, method: str):
    class MyClass(FluentBaseModel):
        normal_field: str
        secret_field: SecretStr
        config_field: ConfigStr

    monkeypatch.setenv("MY_ENV_VAR", "dont_serialize_me")

    m = MyClass(
        normal_field="normal",
        secret_field="secret",  # type: ignore[arg-type]
        config_field=r"${MY_ENV_VAR}",  # type: ignore[arg-type]
    )
    serialize_method: Callable = getattr(m, method)
    dumped = str(serialize_method())
    assert "dont_serialize_me" not in dumped
    assert r"${MY_ENV_VAR}" in dumped


@pytest.mark.parametrize("method", ["yaml", "dict", "json"])
def test_nested_serialization(monkeypatch: MonkeyPatch, method: str):
    class MyClass(FluentBaseModel):
        normal_field: str
        secret_field: SecretStr
        config_field: ConfigStr

    class MyCollection(FluentBaseModel):
        my_classes: List[MyClass] = []

    MyCollection.update_forward_refs(MyClass=MyClass)

    monkeypatch.setenv("MY_ENV_VAR", "dont_serialize_me")

    m = MyCollection(
        my_classes=[
            MyClass(
                normal_field="normal",
                secret_field="secret",  # type: ignore[arg-type]
                config_field=r"${MY_ENV_VAR}",  # type: ignore[arg-type]
            )
        ]
    )
    serialize_method: Callable = getattr(m, method)
    dumped = str(serialize_method())
    assert "dont_serialize_me" not in dumped
    assert r"${MY_ENV_VAR}" in dumped


class TestSecretMasking:
    def test_validation_error(self):
        pass

    def test_repr(self):
        pass

    def test_str(self):
        pass

    def test_serialization(self):
        pass
