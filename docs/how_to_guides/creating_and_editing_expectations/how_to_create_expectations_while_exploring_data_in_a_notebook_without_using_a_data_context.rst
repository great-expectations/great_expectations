.. _how_to_guides__creating_and_editing_expectations__how_to_create_expectations_while_exploring_data_in_a_notebook_without_using_a_data_context:

How to create Expectations while exploring data in a notebook, without using a Data Context.
============================================================================================

This guide will help you get started quickly in Great Expectations, without even setting up a :ref:`Data Context`. All you need is a notebook and some data.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - Installed Great Expectations (e.g. ``pip install great_expectations``)
    - Have access to a notebook (e.g. ``jupyter notebook``, ``jupyter lab``, etc.)
    - Be able to access data from your notebook
    - Nothing else

    Unlike most how-to guides, these instructions do *not* assume that you have configured a Data Context by running ``great_expectations init``. You can find more guides like this here: “reference/A magic-free introduction to GE”

Steps
-----

All of these steps take place within your notebook:

1. **Import Great Expectations.**

    .. code-block:: python

        import great_expectations as ge

2. **Load some data.**

    The simplest way to do this is with ``read_csv``.

    .. code-block:: python

        my_df = ge.read_csv("my_data_directory/titanic.csv")

    This method behaves exactly the same as ``pandas.read_csv``, so you can add parameters to parse your file:
    
    .. code-block:: python

        my_df = ge.read_csv(
            "my_data_directory/my_messy_data.csv",
            sep="\t",
            skiprows=3
        )

    Similarly wrapped versions of other pandas methods (``read_excel``, ``read_table``, ``read_parquet``, ``read_pickle``, ``read_json``, etc.) are also available. Please see the <great_expectations utils module> for details.

    If you wish to load data from somewhere else (e.g. from a SQL database or blob store), please fetch a copy of the data locally, or :ref:`configure a Datasource`_. (In the future, you will likely want to configure Datasources anyway, to take advantage of more of <Great Expectations' advanced features>.)

    As another option, if you have already instantiated a ``pandas.Dataframe``, you can use ``from_pandas``:

    .. code-block:: python

        my_df = ge.from_pandas(
            my_pandas_dataframe
        )

    This method will convert your boring old pandas ``DataFrame`` into a new and exciting great_expectations ``PandasDataAsset``. The two classes are absolutely identical, except that ``PandasDataAsset`` has access to Great Expectations' methods.

3. **Explore your data and add Expectations.**

    All of the methods in step 1 will produce ``my_df``, a ``PandasDataAsset``. ``PandasDataAsset`` is a subclass of ``pandas.DataFrame``, which means that you can use all of pandas' normal methods on it.

    .. code-block:: python

        my_df.head()
        my_df.Sex.value_counts()
        my_df[my_df.Sex=="M"].head()
        my_df.group_by(...)
        # etc., etc. 
        
    In addition to these methods, ``my_df`` has access to a wide array of :ref:`Expectations` methods. (You can see the full list here.) By convention, every Expectation method name starts with the name ``expect_...``, so you can quickly access the full list with tab-based autocomplete:

    .. image:: ../../images/expectation_autocomplete.gif

    |

    When you invoke an Expectation, it will immediately evaluate against your data and return a dictionary containing the result and a list of exceptions. This instant feedback helps you zero in on exceptions very quickly, taking a lot of the pain and guesswork out of data exploration.

    .. image:: ../../images/expectation_notebook_interactive_loop.gif

    |

    Hint: it's common to encounter data issues where most cases match, but you can't guarantee 100% adherence. In these cases, consider using a ``mostly`` parameter. This parameter is an option for all <Expectations that are applied on a row-by-row basis>, and allows you to build wiggle room into your data validation.

    .. figure:: ../../images/interactive_mostly.gif

        Note how ``success`` switches from ``false`` to ``true`` once ``mostly=.99`` is added.

    |

4. **Review and save your Expectations.**

    As you run Expectations in your notebook, ``my_df`` will build up a running list of Expectations.

    Great Expectations is smart enough to recognize and replace duplicate Expectations. (For more details, see XXX.)


Additional notes
----------------

Building Expectations as you conduct exploratory data analysis is a great way to ensure that your insights about data processes and pipelines remain part of your team's knowledge.

Great Expectations's library of Expectations has been developed by a broad cross-section of data scientists and engineers. Check out the :ref:`expectation_glossary`; it covers all kinds of practical use cases:

* Foreign key verification and row-based accounting for ETL
* Form validation and regex pattern-matching for names, URLs, dates, addresses, etc.
* Checks for missing data
* Crosstabs
* Distributions for statistical modeling.
* etc.

Adding notes and metadata
~~~~~~~~~~~~~~~~~~~~~~~~~

You can also add notes or even structured metadata to Expectations:

.. code-block:: bash

    >> my_df.expect_column_values_to_match_regex(
        "Name",
        "^[A-Za-z\, \(\)\']+$",
        meta = {
            "notes": "A simple experimental regex for name matching.",
            "source": "max@company.com"
            }
       )

Additional resources
--------------------

- An example notebook.
- Glossary of Expectations


Comments
--------

.. discourse::
    :topic_identifier: 203
