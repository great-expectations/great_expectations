.. _usage_statistics:


#################
Usage Statistics
#################

To help us improve the tool, by default we track event data when certain Data Context-enabled commands are run. The
usage statistics include things like the OS and python version, and which GE features are used. You can see the exact
schemas for all of our messages
`here <https://github.com/great-expectations/great_expectationst/tree/development/great_expectations/core/usage_statistics/schemas.py>`_.

While we hope you'll leave them on, you can easily disable usage statistics for a Data Context by adding the
following to your data context configuration:

.. code-block:: yaml
    anonymized_usage_statistics:
      data_context_id: <randomly-generated-uuid>
      enabled: false

You can also disable usage statistics system-wide by setting the `GE_USAGE_STATS` environment variable to `FALSE` or
adding the following code block to a file called `great_expectations.conf` located in `/etc/` or `~/
.great_expectations`:

.. code-block::

    [anonymous_usage_statistics]
    enabled=FALSE

As always, please reach out `on Slack <https://greatexpectations.io/slack>`__ if you have any questions or comments.
