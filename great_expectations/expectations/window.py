from great_expectations.compatibility import pydantic


class PercentOffset(pydantic.BaseModel):
    positive: float
    negative: float


class Window(pydantic.BaseModel):
    constraint_fn: str
    parameter_name: str
    range: int
    percent_offset: PercentOffset
