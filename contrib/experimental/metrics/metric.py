from typing import Literal
from contrib.experimental.metrics.metric_provider import Metric

class ColumnMeanMetric(Metric):
    name: Literal["ColumnMeanMetric"] = "ColumnMeanMetric"
    column: str

    def id(self):
        return "__".join((
            self.name,
            self.batch_definition.get_id(self.batch_parameters),
            self.column
        ))
    

class ColumnValuesMatchRegexMetric(Metric):
    name: Literal["ColumnValuesMatchRegexMetric"] = "ColumnValuesMatchRegexMetric"
    column: str
    regex: str

    def id(self):
        return "__".join((
            self.name,
            self.batch_definition.get_id(self.batch_parameters),
            self.column,
            self.regex
        ))

class ColumnValuesMatchRegexUnexpectedValuesMetric(Metric):
    name: Literal["ColumnValuesMatchRegexUnexpectedValuesMetric"] = "ColumnValuesMatchRegexUnexpectedValuesMetric"

    column: str
    regex: str
    limit: int = 20

    def id(self):
        return "__".join((
            self.name,
            self.batch_definition.get_id(self.batch_parameters),
            self.column,
            self.regex
        ))
