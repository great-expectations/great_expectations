"""Example Script: How to configure a Spark Datasource

This example script is intended for use in documentation on how to configure a Spark Datasource.

Assert statements are included to ensure that if the behaviour shown in this script breaks it will not pass
tests and will be updated.  These statements can be ignored by users.

Comments with the tags `<snippet>` and `</snippet>` are used to ensure that if this script is updated
the snippets that are specified for use in documentation are maintained.  These comments can be ignored by users.

--documentation--
    https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource
"""

# The following imports are used as part of verifying that all example snippets are consistent.
# Users may disregard them.

from datasource_configuration_test_utilities import is_subset
from how_to_configure_a_datasource_universal_steps import (
    get_full_universal_datasource_config_elements,
)

import great_expectations as gx
