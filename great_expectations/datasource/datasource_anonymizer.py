from great_expectations.core.logging.anonymizer import Anonymizer
from great_expectations.datasource import Datasource, PandasDatasource, SqlAlchemyDatasource, SparkDFDatasource


class DatasourceAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super(DatasourceAnonymizer, self).__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self.ge_datasource_classes = [
            PandasDatasource,
            SqlAlchemyDatasource,
            SparkDFDatasource,
            Datasource
        ]

    def anonymize_datasource_info(self, datasource_obj):
        datasource_config = datasource_obj.config
        datasource_name = datasource_obj.name
        class_name = datasource_config.get("class_name")
        class_ = datasource_obj.__class__

        # Get a baseline:
        anonymized_datasource_info = dict()
        anonymized_datasource_info["anonymized_name"] = self.anonymize(datasource_name)

        for ge_datasource_class in self.ge_datasource_classes:
            if issubclass(class_, ge_datasource_class):
                anonymized_datasource_info["parent_class"] = ge_datasource_class.__name__
                if ge_datasource_class == SqlAlchemyDatasource:
                    sqlalchemy_dialect = datasource_obj.engine.name
                    anonymized_datasource_info["sqlalchemy_dialect"] = sqlalchemy_dialect
                if not class_ == ge_datasource_class:
                    anonymized_datasource_info["anonymized_class"] = self.anonymize(class_name)
                break

        if not anonymized_datasource_info.get("parent_class"):
            anonymized_datasource_info["parent_class"] = "__not_recognized__"
            anonymized_datasource_info["anonymized_class"] = self.anonymize(class_name)

        return anonymized_datasource_info
