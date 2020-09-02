.. _how_to_guides__creating_and_editing_expectations__how_to_edit_an_expectation_suite_using_a_disposable_notebook:

How to edit an Expectation Suite using a disposable notebook
==========================================================================

Editing :ref:`Expectations` in a notebook is usually much more convenient than editing them as raw JSON objects. You can evaluate them against real data, examine the results, and calibrate parameters. Often, you also learn about your data in the process.
    
To simplify this workflow, the CLI command ``suite edit`` takes a named :ref:`Expectation Suite` and uses it to *generate* an equivalent Jupyter notebook. You can then use this notebook as a disposable interface to explore data and edit your Expectations.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`
  - Created an Expectation Suite, possibly using the :ref:`great_expectations suite scaffold <how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_suite_scaffold>` or :ref:`great_expectations suite new <how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli>` commands

Steps
-----

1. Generate a notebook.

    To edit a suite called movieratings.ratings you could run the CLI command:

    .. code-block:: bash

        great_expectations suite edit movieratings.ratings

    For convenience, the :ref:`Data Docs` page for each Expectation Suite has the CLI command syntax for you. Simply press the “How to Edit This Suite” button, and copy/paste the CLI command into your terminal.

    .. image:: /images/edit_e_s_popup.png

2. Run the boilerplate code at the top of the notebook.

    The first code cell at the top of the notebook contains boilerplate code to fetch your Expectation Suite, plus a Batch of data to test it on.
    
    Run this cell to get started.

3. Run (and edit) any Expectations in the "Create & Edit Expectations" section:

    The middle portion of the notebook contains a code cell to reproduce each Expectation in the Suite.
    
    - If you re-run these cells with no changes, you will re-validate all of your Expectations against the chosen Batch. At the end of your notebook, the Expectation Suite will contain exactly the same Expectations that you started out with.
    - If you edit and run any of the Expectations, the new parameters will replace the old ones.
    - If you don't run an Expectation, it will be left out of the Suite.
    |
    Until you run the final cell in the notebook, any changes will only affect the Expectation Suite in memory. They won't yet be saved for later.
    
4. Save your Expectations and review them in Data Docs

    At the bottom of the notbeook, there's a cell labeled "Save & Review Your Expectations." It will:

        #. Save your Expectations back to the configured Expectation Store.
        #. Validate the batch, using a Validation Operator. (This will automatically store Validation Results.)
        #. Rebuild Data Docs.
        #. Open your Data Docs to the appropriate validation page.


5. Delete the notebook

    In general, these Jupyter notebooks should not be kept in source control. In almost all cases, it's better to treat the Expectations as the source of truth, and delete the notebook to avoid confusion. (You can always auto-generate another one later.)

    The notebook will be stored in the ``great_expectations/uncommitted`` directory. You can remove it like so:

    .. code-block:: bash

        rm great_expectations/uncommitted/edit_movieratings.ratings.ipynb


Content
-------

.. discourse::
    :topic_identifier: 200
