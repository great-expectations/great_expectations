.. conditional_expectations:

##############################
Conditional Expectations
##############################

This reference provides some examples of conditional expectations applied to a real dataset. Additional
code fragments and examples can be found in the :ref:`conditional_expectations` feature guide.

All examples use the `NPI dataset <https://download.cms.gov/nppes/NPI_Files.html>`_.

Example 1:
==========

One common use case for conditional expectations is to test that values in multiple columns are consistent with each other.

One can test, for example, whether abbreviated state names are correct conditional on the unit being from the U.S. or the U.S. Minor Outlying Islands:

.. code-block:: bash

    >>> import great_expectations as ge
    >>> my_df = ge.read_csv("data/npidata_cut_65k.csv")
    >>> my_df.expect_column_values_to_be_in_set(
            column='Provider Business Mailing Address State Name',
            value_set=['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
                       'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                       'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                       'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                       'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'] + ['PR', 'VI', 'GU', 'AE'],
            row_condition='`Provider Business Mailing Address Country Code (If outside U.S.)` in ["US", "UM"]'
        )
    {
        "success": false,
        "result": {
            "element_count": 61733,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 5,
            "unexpected_percent": 0.008099395785074433,
            "unexpected_percent_nonmissing": 0.008099395785074433,
            "partial_unexpected_list": [
                "PUERTO RICO",
                "PUERTO RICO",
                "PUERTO RICO",
                "GUAM",
                "PUERTO RICO"
            ]
        }
    }

This shows that some of the states are given with their full names rather than their abbreviations.


Example 2:
==========
One can also test whether name prefixes are consistent with gender:

.. code-block:: bash

    >>> my_df.expect_column_values_to_be_in_set(
            column='Provider Name Prefix Text',
            value_set=['MRS.', 'MS.', 'MISS', 'DR.', 'PROF.'],
            row_condition='`Provider Gender Code`=="F"'
        )
    {
        "success": false,
        "result": {
            "element_count": 15051,
            "missing_count": 7640,
            "missing_percent": 50.76074679423294,
            "unexpected_count": 7,
            "unexpected_percent": 0.0465085376386951,
            "unexpected_percent_nonmissing": 0.09445418971798678,
            "partial_unexpected_list": [
                "MR.",
                "MR.",
                "MR.",
                "MR.",
                "MR.",
                "MR.",
                "MR."
            ]
        }
    }


Example 3:
==========

Conditional Expectations can also be used to test for alternative non-missingness. In the NPI dataset, for example, a provider can either be a person with a last name or an organization, in which case the last name column should be missing:

.. code-block:: bash

    >>> my_df.expect_column_values_to_be_null(
            column='Provider Last Name (Legal Name)',
            row_condition='`Provider Organization Name (Legal Business Name)`.notnull()'
        )
    {
        "success": true,
        "result": {
            "element_count": 10515,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": []
        }
    }

    >>> my_df.expect_column_values_to_not_be_null(
            column='Authorized Official Last Name',
            row_condition='`Provider Organization Name (Legal Business Name)`.notnull()'
        )
    {
        "success": true,
        "result": {
            "element_count": 10515,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": []
        }
    }
