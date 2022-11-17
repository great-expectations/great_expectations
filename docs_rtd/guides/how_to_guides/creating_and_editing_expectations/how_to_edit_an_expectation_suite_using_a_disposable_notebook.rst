.. _how_to_guides__creating_and_editing_expectations__how_to_edit_an_expectation_suite_using_a_disposable_notebook:

How to edit an Expectation Suite using a disposable notebook
==========================================================================

Editing :ref:`Expectations` in a notebook is usually much more convenient than editing them as raw JSON objects. You can evaluate them against real data, examine the results, and calibrate parameters. Often, you also learn about your data in the process.
    
To simplify this workflow, the CLI command ``suite edit`` takes a named :ref:`Expectation Suite <reference__core_concepts__expectations__expectation_suites>` and uses it to *generate* an equivalent Jupyter notebook. You can then use this notebook as a disposable interface to edit your Expectations and optionally explore data.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Created an Expectation Suite, possibly using the :ref:`great_expectations suite scaffold <how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_suite_scaffold>` or :ref:`great_expectations suite new <how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli>` commands

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. Generate a notebook.

            To edit a suite called movieratings.warning you could run the CLI command:

            .. code-block:: bash

                great_expectations suite edit movieratings.warning

            For convenience, the :ref:`Data Docs <reference__core_concepts__datasources>` page for each Expectation Suite has the CLI command syntax for you. Simply press the “How to Edit This Suite” button, and copy/paste the CLI command into your terminal.

            .. image:: /images/edit_e_s_popup.png

        2. Run the boilerplate code at the top of the notebook.

            The first code cell at the top of the notebook contains boilerplate code to fetch your Expectation Suite, plus a Batch of data to test it on.

            Run this cell to get started.

        3. Run (and edit) any Expectations in the "Create & Edit Expectations" section:

            The middle portion of the notebook contains a code cell to reproduce each Expectation in the Suite.

            - If you re-run these cells with no changes, you will re-validate all of your Expectations against the chosen Batch. At the end of your notebook, the Expectation Suite will contain exactly the same Expectations that you started out with.
            - If you edit and run any of the Expectations, the new parameters will replace the old ones.
            - If you don't run an Expectation, it will be left out of the Suite.

            Until you run the final cell in the notebook, any changes will only affect the Expectation Suite in memory. They won't yet be saved for later.

        4. Save your Expectations and review them in Data Docs

            At the bottom of the notebook, there's a cell labeled "Save & Review Your Expectations." It will:

                #. Save your Expectations back to the configured Expectation Store.
                #. Validate the batch, using a Validation Operator. (This will automatically store Validation Results.)
                #. Rebuild Data Docs.
                #. Open your Data Docs to the appropriate validation page.


        5. Delete the notebook

            In general, these Jupyter notebooks should not be kept in source control. In almost all cases, it's better to treat the Expectations as the source of truth, and delete the notebook to avoid confusion. (You can always auto-generate another one later.)

            The notebook will be stored in the ``great_expectations/uncommitted`` directory. You can remove it like so:

            .. code-block:: bash

                rm great_expectations/uncommitted/edit_movieratings.warning.ipynb


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API Without a Batch of data

        This mode of Great Expectations allows you to edit your Expectation Suite even when the Batch of data is unavailable (or not needed) at the time.

        1. Generate a notebook.

            To edit a suite called ``movieratings.warning`` you could run the CLI command:

            .. code-block:: bash

                great_expectations --v3-api suite edit movieratings.warning

            For convenience, the :ref:`Data Docs <reference__core_concepts__datasources>` page for each Expectation Suite has the CLI command syntax for you.

        2. Run the boilerplate code at the top of the notebook.

            The first code cell at the top of the notebook contains boilerplate code to fetch your Expectation Suite for you to edit in the subsequent cells.

            Run this cell to get started.

        3. Run (and edit) any Expectations in the "Create & Edit Expectations" section:

            The middle portion of the notebook contains a code cell to reproduce each Expectation in the Suite.

            - If you re-run these cells with no changes, you will simply recreate all of your existing Expectations (without validations, since there is no Batch of data). At the end of your notebook, the Expectation Suite will contain exactly the same Expectations that you started out with.
            - If you edit and run any of the Expectations, the new parameters will replace the old ones.
            - If you don't run an Expectation, it will be left out of the Suite.
            - You are welcome to modify the existing Expectations and/or add new ones using the `ExpectationConfiguration` syntax (see :ref:`How to create a new Expectation Suite without a sample Batch <how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_without_a_sample_batch>`).

            Until you run the final cell in the notebook, any changes will only affect the Expectation Suite in memory. They won't yet be saved for later.

        4. Save your Expectations and review them in Data Docs

            At the bottom of the notebook, there's a cell labeled "Save & Review Your Expectations." It will:

                #. Save your Expectations back to the configured Expectation Store.
                #. Rebuild Data Docs.
                #. Open your Data Docs to the ExpectationSuite page for your review.  (Note: since there is no Batch of data in this mode, validations are not run. Nevertheless, when the Batch of data is available, all your expectations will appear intact, with the ability to run validations in the notebook.)

        5. Delete the notebook

            In general, these Jupyter notebooks should not be kept in source control. In almost all cases, it's better to treat the Expectations as the source of truth, and delete the notebook to avoid confusion. (You can always auto-generate another one later.)

            The notebook will be stored in the ``great_expectations/uncommitted`` directory. You can remove it like so:

            .. code-block:: bash

                rm great_expectations/uncommitted/edit_movieratings.warning.ipynb

    .. tab-container:: tab2
        :title: Show Docs for V3 (Batch Request) API With a Batch of data

        This mode of Great Expectations allows you to edit your Expectation Suite even when access to data is available.

        1. Generate a notebook.

            To edit a suite called ``movieratings.warning`` you could run the CLI command:

            .. code-block:: bash

                great_expectations --v3-api suite edit movieratings.warning --interactive

            For convenience, the :ref:`Data Docs <reference__core_concepts__datasources>` page for each Expectation Suite has the CLI command syntax for you. Simply press the “How to Edit This Suite” button, and copy/paste the CLI command into your terminal.

        2. Run the boilerplate code at the top of the notebook.

            The first code cell at the top of the notebook contains boilerplate code to fetch your Expectation Suite, plus a ready-to-use `BatchRequest` dictionary needed to obtain the Batch of data to test it on.

            Run this cell to get started.

        3. Run (and edit) any Expectations in the "Create & Edit Expectations" section:

            The middle portion of the notebook contains a code cell to reproduce each Expectation in the Suite.

            - If you re-run these cells with no changes, you will re-validate all of your Expectations against the chosen Batch. At the end of your notebook, the Expectation Suite will contain exactly the same Expectations that you started out with.
            - If you edit and run any of the Expectations, the new parameters will replace the old ones.
            - If you don't run an Expectation, it will be left out of the Suite.
            - You are welcome to modify the existing Expectations and/or add new ones using the familiar
              `validator.expect_` syntax.

            Until you run the final cell in the notebook, any changes will only affect the Expectation Suite in memory. They won't yet be saved for later.

        4. Save your Expectations and review them in Data Docs

            At the bottom of the notebook, there's a cell labeled "Save & Review Your Expectations." It will:

                #. Save your Expectations back to the configured Expectation Store.
                #. Validate the batch, using the Validator object (the `validator` reference in your notebook). This will automatically store Validation Results.
                #. Rebuild Data Docs.
                #. Open your Data Docs to the appropriate validation results page.

        5. Delete the notebook

            In general, these Jupyter notebooks should not be kept in source control. In almost all cases, it's better to treat the Expectations as the source of truth, and delete the notebook to avoid confusion. (You can always auto-generate another one later.)

            The notebook will be stored in the ``great_expectations/uncommitted`` directory. You can remove it like so:

            .. code-block:: bash

                rm great_expectations/uncommitted/edit_movieratings.warning.ipynb



Content
-------

.. discourse::
    :topic_identifier: 200
