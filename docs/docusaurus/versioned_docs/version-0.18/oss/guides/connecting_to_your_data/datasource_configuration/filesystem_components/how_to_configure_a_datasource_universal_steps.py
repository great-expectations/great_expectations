# ruff: noqa: F841
"""Example Script: How to configure a Spark/Pandas/Sql Datasource (universal configuration elements)

This example script is intended for use in documentation on how to configure Datasources.  It contains the top level
configuration items that are identical for Spark, Pandas, and Sql.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource
"""


def section_1_import_necessary_modules_and_initialize_your_data_context():
    """Provides and tests the snippets for section 1 of the Spark, Pandas, and SQL Datasource configuration guides.

    Returns:
        a Great Expectations DataContext object
    """
    # <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/datasource_configuration/filesystem_components/how_to_configure_a_datasource_universal_steps imports and data context">
    import great_expectations as gx
    from great_expectations.core.yaml_handler import YAMLHandler

    yaml = YAMLHandler()
    data_context = gx.get_context()
    # </snippet>

    return data_context


section_1_import_necessary_modules_and_initialize_your_data_context()
