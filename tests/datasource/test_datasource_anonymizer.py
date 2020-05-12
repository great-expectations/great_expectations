from great_expectations.core.usage_statistics.anonymizers.datasource_anonymizer import (
    DatasourceAnonymizer,
)
from great_expectations.datasource import PandasDatasource


class CustomDatasource(PandasDatasource):
    pass


def test_datasource_anonymizer():
    datasource_anonymizer = DatasourceAnonymizer()
    # n1 = datasource_anonymizer.anonymize_datasource_info("PandasDatasource")
    # assert n1 == {"parent_class": "PandasDatasource"}
    #
    # n2 = datasource_anonymizer.anonymize_datasource_info("CustomDatasource")
    # datasource_anonymizer_2 = DatasourceAnonymizer()
    # n3 = datasource_anonymizer_2.anonymize_datasource_info("CustomDatasource")
    # assert n2["parent_class"] == "PandasDatasource"
    # assert n3["parent_class"] == "PandasDatasource"
    # assert len(n3["custom_class"]) == 32
    # assert n2["custom_class"] != n3["custom_class"]
    #
    # # Same anonymizer *does* produce the same result
    # n4 = datasource_anonymizer.anonymize_datasource_info("CustomDatasource")
    # assert n4["custom_class"] == n2["custom_class"]
