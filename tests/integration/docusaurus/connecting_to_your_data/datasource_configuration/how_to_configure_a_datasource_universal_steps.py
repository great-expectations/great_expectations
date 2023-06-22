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

# The following imports are used as part of verifying that all example snippets are consistent.
# Users may disregard them.

from datasource_configuration_test_utilities import is_subset
from full_datasource_configurations import (
    get_partial_config_universal_datasource_config_elements,
)

import great_expectations as gx

# The following methods correspond to the section headings in the how-to guide linked in the module docstring.


def section_1_import_necessary_modules_and_initialize_your_data_context() -> (
    gx.DataContext
):
    """Provides and tests the snippets for section 1 of the Spark, Pandas, and SQL Datasource configuration guides.

    Returns:
        a Great Expectations DataContext object
    """
    # <snippet name="import necessary modules and initialize your data context">
    import great_expectations as gx
    from great_expectations.core.yaml_handler import YAMLHandler

    yaml = YAMLHandler()
    data_context: gx.DataContext = gx.get_context()
    # </snippet>

    assert isinstance(data_context, gx.DataContext)
    return data_context


def section_2_create_new_datasource_configuration():
    """Provides and tests the snippets for section 2 of the Spark, Pandas, and SQL Datasource configuration guides."""
    # <snippet name="create empty datasource_config dictionary">
    datasource_config: dict = {}
    # </snippet>
    is_subset(
        datasource_config, get_partial_config_universal_datasource_config_elements()
    )


def section_3_name_your_datasource():
    """Provides and tests the snippets for section 3 of the Spark, Pandas, and SQL Datasource configuration guides."""
    # <snippet name="datasource_config up to name being populated as my_datasource_name">
    datasource_config: dict = {"name": "my_datasource_name"}
    # </snippet>

    name_snippet: dict = {
        # <snippet name="populate name as my_datasource_name">
        "name": "my_datasource_name"
        # </snippet>
    }
    assert name_snippet == datasource_config
    is_subset(
        datasource_config, get_partial_config_universal_datasource_config_elements()
    )


def section_4_specify_the_datasource_class_and_module():
    """Provides and tests the snippets for section 4 of the Spark, Pandas, and SQL Datasource configuration guides."""
    # <snippet name="datasource_config containing top level elements universal to spark pandas and sql Datasources">
    datasource_config: dict = {
        "name": "my_datasource_name",  # Preferably name it something relevant
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
    }
    # </snippet>
    assert (
        datasource_config == get_partial_config_universal_datasource_config_elements()
    )
    is_subset(
        datasource_config, get_partial_config_universal_datasource_config_elements()
    )


section_1_import_necessary_modules_and_initialize_your_data_context()
section_2_create_new_datasource_configuration()
section_3_name_your_datasource()
section_4_specify_the_datasource_class_and_module()
