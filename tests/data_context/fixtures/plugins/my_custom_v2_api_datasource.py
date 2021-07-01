from great_expectations.datasource import PandasDatasource


class MyCustomV2ApiDatasource(PandasDatasource):
    def self_check(self, pretty_print):
        pass
