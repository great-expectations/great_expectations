from great_expectations.datasource import PandasDatasource
from great_expectations.datasource.datasource_anonymizer import DatasourceAnonymizer


class CustomDatasource(PandasDatasource):
    pass


def test_datasource_anonymizer():
    datasource_anonymizer = DatasourceAnonymizer()
    n1 = datasource_anonymizer.anonymize_datasource_class_name("PandasDatasource", "great_expectations.datasource")
    assert n1 == {"parent_class": "PandasDatasource"}

    n2 = datasource_anonymizer.anonymize_datasource_class_name("CustomDatasource", module_name=__name__)
    datasource_anonymizer_2 = DatasourceAnonymizer()
    n3 = datasource_anonymizer_2.anonymize_datasource_class_name("CustomDatasource", module_name=__name__)
    assert n2["parent_class"] == "PandasDatasource"
    assert n3["parent_class"] == "PandasDatasource"
    assert len(n3["custom_class"]) == 32
    assert n2["custom_class"] != n3["custom_class"]

    # Same anonymizer *does* produce the same result
    n4 = datasource_anonymizer.anonymize_datasource_class_name("CustomDatasource", module_name=__name__)
    assert n4["custom_class"] == n2["custom_class"]
