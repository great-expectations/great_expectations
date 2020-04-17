from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.datasource import Datasource, PandasDatasource, SqlAlchemyDatasource, SparkDFDatasource


class DatasourceAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super(DatasourceAnonymizer, self).__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            PandasDatasource,
            SqlAlchemyDatasource,
            SparkDFDatasource,
            Datasource
        ]

    def anonymize_datasource_info(self, datasource_obj):
        anonymized_info_dict = dict()
        name = datasource_obj.name
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        self.anonymize_object_info(
            object_=datasource_obj,
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes
        )

        if anonymized_info_dict.get("parent_class") == "SqlAlchemyDatasource":
            sqlalchemy_dialect = datasource_obj.engine.name
            anonymized_info_dict["sqlalchemy_dialect"] = sqlalchemy_dialect

        return anonymized_info_dict
