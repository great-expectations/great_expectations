from great_expectations.core.logging.anonymizer import Anonymizer
from great_expectations.datasource import Datasource, PandasDatasource, SqlAlchemyDatasource, SparkDFDatasource


class DatasourceAnonymizer(Anonymizer):
    def anonymize_datasource_info(self, datasource_obj):
        datasource_config = datasource_obj.config
        datasource_name = datasource_obj.name
        class_name = datasource_config.get("class_name")
        class_ = datasource_obj.__class__

        # Get a baseline:
        anonymized_datasource_info = dict()
        if issubclass(class_, Datasource):
            anonymized_datasource_info["parent_class"] = "Datasource"
        else:
            anonymized_datasource_info["parent_class"] = "__not_datasource__"
            anonymized_datasource_info["anonymized_class"] = self.anonymize(class_name)

        if issubclass(class_, PandasDatasource):
            anonymized_datasource_info["parent_class"] = "PandasDatasource"
            if not class_ == PandasDatasource:
                anonymized_datasource_info["anonymized_class"] = self.anonymize(class_name)
        elif issubclass(class_, SqlAlchemyDatasource):
            sqlalchemy_dialect = datasource_obj.engine.name
            anonymized_datasource_info["parent_class"] = "SqlAlchemyDatasource"
            anonymized_datasource_info["sqlalchemy_dialect"] = sqlalchemy_dialect
            if not class_ == SqlAlchemyDatasource:
                anonymized_datasource_info["anonymized_class"] = self.anonymize(class_name)
        elif issubclass(class_, SparkDFDatasource):
            anonymized_datasource_info["parent_class"] = "SparkDFDatasource"
            if not class_ == SparkDFDatasource:
                anonymized_datasource_info["anonymized_class"] = self.anonymize(class_name)

        anonymized_datasource_info["anonymized_name"] = self.anonymize(datasource_name)
        if not anonymized_datasource_info.get("parent_class"):
            anonymized_datasource_info["parent_class"] = "__unrecognized__"
            anonymized_datasource_info["anonymized_class"] = self.anonymize(class_name)

        return anonymized_datasource_info
