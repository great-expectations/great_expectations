from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pydantic import SecretStr

from great_expectations.core.config_substitutor import TEMPLATE_STR_REGEX
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel

if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider


class ConfigStr(SecretStr):
    def __init__(
        self,
        template_str: str,
        config_provider: _ConfigurationProvider | None = None,
    ) -> None:
        # TODO: do the config sub immediately and save it to `_secret_value`?
        # or do at at access time in `get_secret_value`
        self.template_str: str = template_str
        self.config_provider: _ConfigurationProvider | None = config_provider
        self._secret_value: str = self._config_sub(template_str)

    def _config_sub(self, template_str: str) -> str:
        # TODO: do the config sub
        return "my_real_config_value"

    def get_config_value(self) -> str:
        # alias for get_secret_value
        return self.get_secret_value()

    def _display(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.template_str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._display()!r})"

    @classmethod
    def _validate_template_str_format(cls, v):
        print(f"_validate_template_str_format - {v}")
        if TEMPLATE_STR_REGEX.match(v):
            return v
        raise ValueError(
            cls.__name__
            + r" - contains no config template strings in the format '${MY_CONFIG_VAR}'"
        )

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls._validate_template_str_format
        yield cls.validate


if __name__ == "__main__":

    class MyClass(FluentBaseModel):
        normal_field: str
        secret_field: SecretStr
        config_field: ConfigStr
        invalid_config_field: Optional[ConfigStr] = None

    for v in MyClass.__get_validators__():
        print(v)

    m = MyClass(
        normal_field="normal",
        secret_field="secret",  # type: ignore[arg-type]
        config_field=r"${MY_CONFIG}",  # type: ignore[arg-type]
        # invalid_config_field="no template",  # type: ignore[arg-type]
    )
    print(m)
    print(m.secret_field)
    print(m.config_field)
    assert str(m.config_field) == r"${MY_CONFIG}", str(m.config_field)
    # TODO: dump the original value to yaml
    print("dict\n", m.dict())
    print(
        "json\n",
        m.json(),
    )
    # print(
    #     "json\n",
    #     m.json(
    #         encoder=lambda v: v.get_secret_value() if v else None,
    #     ),
    # )
    print("yaml\n", m.yaml())
