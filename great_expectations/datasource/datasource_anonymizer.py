from great_expectations.core.logging.anonymizer import Anonymizer
from great_expectations.datasource import Datasource, PandasDatasource, SqlAlchemyDatasource, SparkDFDatasource
from great_expectations.util import load_class


class DatasourceAnonymizer(Anonymizer):
    def anonymize_datasource_class_name(self, class_name, module_name):
        try:
            class_ = load_class(class_name=class_name, module_name=module_name)

            # Get a baseline:
            anonymized_class_info = dict()
            if issubclass(class_, Datasource):
                anonymized_class_info["parent_class"] = "Datasource"
            else:
                anonymized_class_info["parent_class"] = "__not_datasource__"
                anonymized_class_info["custom_class"] = self.anonymize(class_name)

            if issubclass(class_, PandasDatasource):
                anonymized_class_info["parent_class"] = "PandasDatasource"
                if not class_ == PandasDatasource:
                    anonymized_class_info["custom_class"] = self.anonymize(class_name)
            elif issubclass(class_, SqlAlchemyDatasource):
                anonymized_class_info["parent_class"] = "SqlAlchemyDatasource"
                if not class_ == SqlAlchemyDatasource:
                    anonymized_class_info["custom_class"] = self.anonymize(class_name)
            elif issubclass(class_, SparkDFDatasource):
                anonymized_class_info["parent_class"] = "SparkDFDatasource"
                if not class_ == SparkDFDatasource:
                    anonymized_class_info["custom_class"] = self.anonymize(class_name)
            return anonymized_class_info
        except AttributeError:
            return {"parent_class": "__unrecognized__", "custom_class": self.anonymize(class_name)}