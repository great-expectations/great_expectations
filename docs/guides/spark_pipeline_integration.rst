.. _tutorial_pipeline_integration:

Integrating Great Expectations into a Spark Pipeline
====================================================================

This section covers integrating Great Expectations (GE) into a data pipeline developed on top of Apache Spark.

The overarching idea is to insert the so-called "GE Data Taps" (or "GE Taps" for short) into the points in your
pipeline, where you are interested in having GE analyze and/or validate data frames at those points.  A GE Data Tap
can focus either on an input data frame or on an output data frame, depending on the needs of the pipeline at the
point where the GE Tap is inserted.  For instance, one GE Tap may be inserted to analyze and validate a source data
set.  Another GE Tap may be inserted after a certain data processing step to analyze and validate the data that is
indented for the next stage of the pipeline. There is no limit to the number of GE Taps.  The number and placement
(or insertion) of GE Data Taps is entirely flexible determined according to the needs of the pipeline and its stages.

A GE Data Tap itself is an API call:

.. code-block:: python

    import great_expectations as ge
    ge.tap(data_asset_name: str, df: DataFrame)

whose two parameters are:

* the **data_asset_name** -- the name of the GE data asset of interest to analyze/validate at the present GE Data Tap
* the **df** -- a **Spark DataFrame** that is the materialization of this GE data asset,

where the GE data asset and its materialization in the form of the corresponding **Spark DataFrame** are tightly
associated with one another.

For example, the value of ``data_asset_name`` can be the name of a table (or a temporary view) in Spark SQL, while the
``df`` will hence be the **Spark DataFrame** materialization of this table (or temporary view).

Once the above GE Tap API call is placed into the pipeline code, no additional code changes for this particular GE Tap
are needed, because the actions of a GE Data Tap are defined in the configuration files, which contain sections
which define expectation suites for each ``data_asset_name`` (identified by convention, such as
"expectation_suite-``data_asset_name``"). This is highly advantageous from the standpoint of code maintenance and unit
testing.  Moreover, quite importantly, the domain experts, who are not necessarily engineers, can now fully take on the
responsibilities for data exploration and validation, without asking engineers to make code changes every time the
domain experts decide to add a new expectation to the suite.  By making changes in the JSON-based configuration files
alone (or with the help of a Web-based configuration UI in the premium version), the GE Data Tap can be directed to:

* Profile the data asset under consideration (i.e., calculate statistics on the **data_frame** that is the ``df`` argument of the GE Tap) and generate documentation for this **data asset**;
* Add and/or edit the expectations in the expectation suites that associated with this **data asset**;
* Validate the data asset during production runs of the pipeline; and
* Choose an action (e.g., halt the pipeline by throwing a descriptive Exception, and/or log a warning to a file, send a message to a Slack channel or PagerDuty, etc.) when validations fail in production.
* Advanced actions could even attempt to repair the failing entries based on the user guidelines.

Example of Expectation Suite JSON Configuration File
----------------------------------------------------

.. code-block:: json

  {
    "data_asset_name": "ge_test/default/npi",
    "expectation_suite_name": "expectation_suite-npi",
    "meta": {
      "great_expectations.__version__": "0.7.7"
    },
    "expectations": [
      {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {
          "column": "claim_number"
        }
      },
      {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {
          "column": "claim_number"
        }
      }
    ],
    "data_asset_type": "Dataset"
}

