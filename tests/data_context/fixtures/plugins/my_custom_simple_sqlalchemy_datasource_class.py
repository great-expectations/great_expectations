from great_expectations.datasource import SimpleSqlalchemyDatasource


class MyCustomSimpleSqlalchemyDatasource(SimpleSqlalchemyDatasource):
    """
    This class is used only for testing.
    E.g. ensuring appropriate usage stats messaging when using plugin functionality.
    """

    pass
