from pydantic import SecretField, SecretStr

from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel


def config_display(secret_field: SecretField) -> str:
    return "**********" if secret_field.get_secret_value() else ""


class ConfigStr(SecretStr):
    def __init__(self, secret_value: str) -> None:
        # TODO: do the config sub immediately and save it to `_secret_value`?
        # or do at at access time in `get_secret_value`
        self._secret_value: str = secret_value

    def get_secret_value(self) -> str:
        # TODO: do the config sub here??
        return super().get_secret_value()

    def get_config_value(self) -> str:
        # alias for get_secret_value
        return self.get_secret_value()

    def _display(self) -> str:
        # TODO: show the original value
        return config_display(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._display()!r})"

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls.validate


if __name__ == "__main__":

    class MyClass(FluentBaseModel):
        normal_field: str
        secret_field: SecretStr
        config_field: ConfigStr

    for v in MyClass.__get_validators__():
        print(v)

    m = MyClass(
        normal_field="normal",
        secret_field="secret",  # type: ignore[arg-type]
        config_field=r"{MY_CONFIG}",  # type: ignore[arg-type]
    )
    print(m)
    print(m.dict())
    print(m.json())
    print(m.yaml())
