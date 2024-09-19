from great_expectations.compatibility import pydantic


class Offset(pydantic.BaseModel):
    """
    A threshold in which a metric will be considered passable
    """

    positive: float
    negative: float


class Window(pydantic.BaseModel):
    """
    A definition for a temporal window across <`range`> number of previous invocations
    """

    constraint_fn: str
    parameter_name: str
    range: int
    offset: Offset
