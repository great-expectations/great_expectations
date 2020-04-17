.. _using_ge_on_teams:

#################################
Using Great Expectations on Teams
#################################

Now that you've enjoyed single player mode, let's bring Great Expectations to
your team.

When you move from single user to collaborative use there are a few major things
to consider.

Major Considerations
===================================

Where Should Expectations Live?
----------------------------------

If you followed our best practice recommendation of committing the
``great_expectations`` directory to your source control repository, then this
question is already answered! Expectations live right in your repo!

It is also possible to store expectations on cloud storage providers--we recommend enabling versioning in that case.
See the documentation on :ref:`customizing the data docs store backend <customizing_data_docs_store_backend>` for
more information, or follow the :ref:`tutorial <publishing_data_docs_to_s3>`.

Where Should Validations Live?
----------------------------------

When using the default Validation Operators, Validations are stored in your
``great_expectations/uncommitted/validations/`` directory. Because these may
include examples of data (which could be sensitive or regulated) these
Validations **should not** be committed to a source control system.

You can configure a Store to write these to a cloud provider blob storage such
as Amazon S3, Google Cloud Storage, or some other securely mounted file system. This will depend on
your team's deployment patterns.

Where Should Data Docs Live?
----------------------------------

Similar to Validations, Data Docs are by default stored in your
``great_expectations/uncommitted/data_docs/`` directory. Because these may
include examples of data (which could be sensitive or regulated) these
**should not** be committed to a source control system.

You can configure a store to write these to a cloud provider blob storage such
as Amazon S3, Google Cloud Storage, or some other securely mounted file system.

See the :ref:`data docs reference <data_docs_reference>` for more information on configuring data docs to use cloud
storage, or follow the :ref:`tutorial <publishing_data_docs_to_s3>` to configure a site now.

Where Should Notifications Go?
----------------------------------

Some teams enjoy realtime data quality alerts in Slack. We find setting up a
channel with an obvious name like ``data-qualty-notifications`` a nice place
to have Great Expectations post to.

How Do You On-board a New Teammate?
===================================

If you have a project in a repo that includes your ``great_expectations``
directory, onboarding is simple! Your teammates will need to:

1. Clone the repo.
2. Run ``great_expectations init`` to create any missing directories.
3. Add any secrets required by your datasources to the file
   ``great_expectations/uncommitted/config_variables.yml``.
