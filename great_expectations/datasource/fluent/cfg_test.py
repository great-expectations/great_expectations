from typing import Optional

import great_expectations as gx
from great_expectations.datasource.fluent.config_str import ConfigStr, SecretStr
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel

context = gx.get_context(cloud_mode=False)
# context.config_provider


class MyClass(FluentBaseModel):
    normal_field: str
    secret_field: SecretStr
    config_field: ConfigStr
    invalid_config_field: Optional[ConfigStr] = None


# for v in MyClass.__get_validators__():
#     print(v)

m = MyClass(
    normal_field="normal",
    secret_field="secret",  # type: ignore[arg-type]
    config_field=r"${USER}",  # type: ignore[arg-type]
    # invalid_config_field="no template",  # type: ignore[arg-type]
    _config_provider=context.config_provider,
)
print(m)
print(m.secret_field)
print(m.config_field)
assert str(m.config_field) == r"${USER}", str(m.config_field)
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
print("\n----------------\n")
print(m.dict(_substitute_config=True))
