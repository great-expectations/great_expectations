from hashlib import md5

from .datasource import Datasource
from .pandas_datasource import PandasDatasource
from .sqlalchemy_datasource import SqlAlchemyDatasource
from .sparkdf_datasource import SparkDFDatasource
from ..core.logging.anonymizer import Anonymizer
from ..util import load_class


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
                anonymized_class_info["custom_class"] = md5(self._token_hex + class_name).hexdigest()

            if issubclass(class_, PandasDatasource):
                anonymized_class_info["parent_class"] = "PandasDatasource"
                if not class_ == PandasDatasource:
                    anonymized_class_info["custom_class"] = md5(self._token_hex + class_name).hexdigest()
            elif issubclass(class_, SqlAlchemyDatasource):
                anonymized_class_info["parent_class"] = "SqlAlchemyDatasource"
                if not class_ == SqlAlchemyDatasource:
                    anonymized_class_info["custom_class"] = md5(self._token_hex + class_name).hexdigest()
            elif issubclass(class_, SparkDFDatasource):
                anonymized_class_info["parent_class"] = "SparkDFDatasource"
                if not class_ == SparkDFDatasource:
                    anonymized_class_info["custom_class"] = md5(self._token_hex + class_name).hexdigest()
            return anonymized_class_info
        except AttributeError:
            return {"parent_class": "__unrecognized__", "custom_class": md5(self._token_hex + class_name).hexdigest()}
