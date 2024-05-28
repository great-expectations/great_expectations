from great_expectations.compatibility import pydantic


class IdentifierBundleDTO(pydantic.BaseModel):
    name: str
    id: str
