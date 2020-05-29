.. _how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_suite_scaffold:

How to create a new Expectation Suite using ``suite scaffold`` (fat)
====================================================================

``great_expectations suite scaffold`` helps you quickly create of :ref:`Expectation Suites` through an interactive development loop that comnbines :ref:`Profilers` and :ref:`Data Docs`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

Steps
-----

1. **Run `suite scaffold`**. 

    .. code-block:: bash

        great_expectations suite scaffold my_suite

    Much like the ``suite new`` and ``suite edit`` commands, you will be prompted interactively to choose some data from one of your datasources.

    .. code-block:: bash

        Heads up! This feature is Experimental. It may change. Please give us your feedback!

        Would you like to:
            1. choose from a list of data assets in this datasource
            2. enter the path of a data file
        : 1

        Which data would you like to use?
            1. highrise_deals (file)
            Don't see the name of the data asset in the list above? Just type it
        : 1

    Once you choose data asset, you the CLI will open up a jupyter notebook.

    .. code-block:: bash

        [I 15:00:10.509 NotebookApp] JupyterLab extension loaded from /home/ubuntu/anaconda2/envs/py3/lib/python3.7/site-packages/jupyterlab
        [I 15:00:10.509 NotebookApp] JupyterLab application directory is /home/ubuntu/anaconda2/envs/py3/share/jupyter/lab
        [I 15:00:10.516 NotebookApp] Serving notebooks from local directory: /home/ubuntu/Desktop/example-project/great_expectations/uncommitted
        [I 15:00:10.517 NotebookApp] The Jupyter Notebook is running at:
        [I 15:00:10.517 NotebookApp] http://localhost:8888/?token=81f32a4fa3dd51dc400dd1272f21a9990564eab2044f81e0
        [I 15:00:10.517 NotebookApp]  or http://127.0.0.1:8888/?token=81f32a4fa3dd51dc400dd1272f21a9990564eab2044f81e0
        [I 15:00:10.517 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
        [C 15:00:10.525 NotebookApp] 
            
            To access the notebook, open this file in a browser:
                file:///home/ubuntu/Library/Jupyter/runtime/nbserver-26713-open.html
            Or copy and paste one of these URLs:
                http://localhost:8888/?token=81f32a4fa3dd51dc400dd1272f21a9990564eab2044f81e0
            or http://127.0.0.1:8888/?token=81f32a4fa3dd51dc400dd1272f21a9990564eab2044f81e0

2. **Within the notebook, choose columns and Expectations to scaffold.**

    You'll then see jupyter open a scaffolding notebook.

    .. figure:: ../../images/suite-scaffold-notebook.png

    .. important::

        Great Expectations generates working jupyter notebooks.
        This saves you tons of time by avoiding all the necessary boilerplate.

        Because these notebooks can be generated at any time from the expectation suites (stored as JSON) you should **consider the notebooks to be entirely disposable artifacts**.

        They are put in your ``great_expectations/uncommitted`` directory and you can delete them at any time.

    Run the first cell in the notebook that loads the data. You don't need to worry about what's happening there.

    .. code-block:: python

        from datetime import datetime
        import great_expectations as ge
        import great_expectations.jupyter_ux
        from great_expectations.profile import BasicSuiteBuilderProfiler
        from great_expectations.data_context.types.resource_identifiers import (
            ValidationResultIdentifier,
        )

        context = ge.data_context.DataContext()

        expectation_suite_name = "my-new-suite"
        suite = context.create_expectation_suite(
            expectation_suite_name, overwrite_existing=True
        )

        batch_kwargs = {
            "path": "/Users/abe/Desktop/boston-housing/great_expectations/../data/BostonHousing.csv",
            "datasource": "data__dir",
            "data_asset_name": "BostonHousing",
        }
        batch = context.get_batch(batch_kwargs, suite)
        batch.head()


    The next code cell in the notebook presents you with a list of all the columns found in your selected data. Because the scaffolder is not very smart, you will want to edit this suite to tune the parameters and make any adjustments such as removing expectations that don't make sense for your use case.

    .. code-block:: python

        included_columns = [
            'crim',
            'zn',
            'indus',
            'chas',
            'nox',
            'rm',
            'age',
            # 'dis',
            'rad',
            # 'tax',
            'ptratio',
            # 'b',
            # 'lstat',
            # 'medv'
        ]

    To select which columns you want to scaffold expectations on, simply uncomment them to include them.

    .. code-block:: python

        # Wipe the suite clean to prevent unwanted expectations on the batch
        suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
        batch = context.get_batch(batch_kwargs, suite)

        scaffold_config = {
            "included_columns": included_columns,
            # "excluded_columns": [],
            # "included_expectations": [],
            # "excluded_expectations": [],
        }
        suite, evr = BasicSuiteBuilderProfiler().profile(batch, profiler_configuration=scaffold_config)


3. **Generate Data Docs and review the results there**

    Run the next few code cells to see the scaffolded suite in Data Docs.

    You may keep the scaffold notebook open and iterate on the included and excluded columns and expectations to get closer to the kind of suite you want.

Additional notes
----------------

.. important::

    The suites generated here **are not meant to be production suites** - they are scaffolds to build upon.

    Great Expectations will choose which expectations **might make sense** for a column based on the type and cardinality of the data in each selected column.

    You will definitely want to edit the suite to hone it after scaffolding.


Additional resources
--------------------

- `Links in RST <https://docutils.sourceforge.io/docs/user/rst/quickref.html#hyperlink-targets>`_ are a pain.



``great_expectations suite scaffold <SUITE_NAME>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^



``great_expectations suite scaffold <SUITE_NAME> --no-jupyter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you wish to skip opening the scaffolding notebook in juypter you can use this optional flag.

The notebook will be created in your ``great_expectations/uncommitted`` directory.

.. code-block:: bash

    suite scaffold npi_distributions --no-jupyter
    Heads up! This feature is Experimental. It may change. Please give us your feedback!

    Enter the path (relative or absolute) of a data file
    : npi.csv
    To continue scaffolding this suite, run `jupyter notebook uncommitted/scaffold_npi_distributions.ipynb`


