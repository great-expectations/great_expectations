.. _tutorials__getting_started__set_up_data_docs:

Set up Data Docs
================

:ref:`Data Docs` translate :ref:`Expectations`, :ref:`Validation Results`, and other metadata into clean, human-readable documentation. Automatically compiling your data documentation from your data tests in the form of Data Docs guarantees that your documentation will never go stale.

In the previous steps, when you executed the last cell in the Jupyter notebook, Great Expectations used the Expectation Suite you generated to validate the January data batch. It then compiled those validation results to HTML, and opened a browser window with a Data Docs validation results page:

.. figure:: /images/validation_results.png

The validation results page is *descriptive*: it describes how a specific batch of data (in this case, the data we profiled) actually looked when validated. The validation results page shows you both your *Expectation* in human-readable form, as well as the *Observed Value* that was found in the data batch you validated, and a *Status* that tells you whether the observed value *passed* or *failed* the validation.

If you scroll down, you will once again find the Expectation we defined for our ``passenger_count`` column, this time in human-readable format: **"distinct values must belong to this set"**. We also see the observed value for this batch, which is exactly the numbers 1 through 6 that we expected. This makes sense, since we're developing the Expectation using the January data batch.

.. figure:: /images/validation_results_column.png

Feel free to click around and explore Data Docs a little more. You will find two more interesting features:

#. If you click on the *Home* page, you will see a list of all validation runs.
#. The *Home* page also has a tab for your Expectation Suites. The Expectation Suite pages show you the *prescriptive* view of your data.

For now, your static site is built and stored locally. In the last step of the tutorial, we'll explain options for configuring, hosting and sharing it.

In the next step, we will complete the Great Expectations workflow by showing you how to validate a new batch of data with the Expectation Suite you just created!
