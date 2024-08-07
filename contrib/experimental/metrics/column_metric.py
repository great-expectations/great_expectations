from contrib.experimental.metrics.metric import ColumnMetric


from typing import Literal


class ColumnMeanMetric(ColumnMetric):
    name: Literal["ColumnMeanMetric"] = "ColumnMeanMetric"

    @property
    def id(self) -> str:
        return "__".join((
            self.name,
            self.domain.id,
        ))


class ColumnValuesMatchRegexMetric(ColumnMetric):
    name: Literal["ColumnValuesMatchRegexMetric"] = "ColumnValuesMatchRegexMetric"
    regex: str

    @property
    def id(self) -> str:
        return "__".join((
            self.name,
            self.domain.id,
            self.regex
        ))


class ColumnValuesMatchRegexUnexpectedValuesMetric(ColumnMetric):
    name: Literal["ColumnValuesMatchRegexUnexpectedValuesMetric"] = "ColumnValuesMatchRegexUnexpectedValuesMetric"

    regex: str
    limit: int = 20

    @property
    def id(self) -> str:
        return "__".join((
            self.name,
            self.domain.id,
            self.regex
        ))
