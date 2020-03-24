.. _using_conditional_expectations:

##############################
Using Conditional Expectations
##############################

This reference provides some code snippets on using conditional expectations with real dataset. You can also find useful
code fragments in the :ref:`conditional_expectations` feature guide. The dataset used here is part of `NPI <https://download.cms.gov/nppes/NPI_Files.html>`_ (having 65,000 rows).

The most crucial aims of Conditional Expectations is testing consistency or learning new features of data.

Example 1:
==========
You can test if state names actually belong to U.S. or U.S. Minor Outlying Islands:

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
            condition='`Provider Business Mailing Address Country Code (If outside U.S.)` in ["US", "UM"]'
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

This shows that some of the states are provided with their full names.

Example 2:
==========
You can test if organizations have last name (they should not) and at the same time if they have an authorized official:

.. code-block:: bash

    >>> my_df.expect_column_values_to_be_null(
            column='Provider Last Name (Legal Name)',
            condition='`Provider Organization Name (Legal Business Name)`.notnull()'
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
            condition='`Provider Organization Name (Legal Business Name)`.notnull()'
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

Example 3:
==========
You can test if the name prefixes are consistent with gender:

.. code-block:: bash

    >>> my_df.expect_column_values_to_be_in_set(
            column='Provider Name Prefix Text',
            value_set=['MRS.', 'MS.', 'MISS', 'DR.', 'PROF.'],
            condition='`Provider Gender Code`=="F"'
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

Example 4:
==========
You can test if entity type codes are consistent:

.. code-block:: bash

    >>> my_df.expect_column_values_to_be_in_set(
            column='Entity Type Code',
            value_set=[2.0],
            condition='`Provider Organization Name (Legal Business Name)`.notnull()'
        )
    {
        "success": true,
        "result": {
            "element_count": 10515,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": []
        }
    }
