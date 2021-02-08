from lxml import etree

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


class ColumnValuesXmlParseable(ColumnMapMetricProvider):
    condition_metric_name = "column_values.xml_parseable"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def is_xml(val):
            try:
                xml_doc = etree.fromstring(val)
                return True
            except:
                return False

        return column.map(is_xml)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        def is_xml(val):
            try:
                xml_doc = etree.fromstring(val)
                return True
            except:
                return False

        is_xml_udf = F.udf(is_xml, sparktypes.BooleanType())

        return is_xml_udf(column)
