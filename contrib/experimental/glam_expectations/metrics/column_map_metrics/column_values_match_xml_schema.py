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


class ColumnValuesMatchXmlSchema(ColumnMapMetricProvider):
    condition_metric_name = "column_values.match_xml_schema"
    condition_value_keys = ("xml_schema",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, xml_schema, format, **kwargs):
        try:
            xmlschema_doc = etree.fromstring(xml_schema)
            xmlschema = etree.XMLSchema(xmlschema_doc)
        except etree.ParseError:
            raise
        except:
            raise
        
        def matches_xml_schema(val):
            try:
                xml_doc = etree.fromstring(val)
                return xmlschema(xml_doc)
            except:
                raise

        return column.map(matches_xml_schema)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, xml_schema, **kwargs):
        try:
            xmlschema_doc = etree.fromstring(xml_schema)
            xmlschema = etree.XMLSchema(xmlschema_doc)
        except etree.ParseError:
            raise
        except:
            raise

        def matches_xml_schema(val):
            if val is None:
                return False
            try:
                xml_doc = etree.fromstring(val)
                return xmlschema(xml_doc)
            except:
                raise

        matches_xml_schema_udf = F.udf(matches_xml_schema, sparktypes.BooleanType())

        return matches_xml_schema_udf(column)
