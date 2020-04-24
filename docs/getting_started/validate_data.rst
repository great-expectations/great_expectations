.. _getting_started__validate_data:

Validating Data using Great Expectations
=========================================================

So far, your team members used Great Expectations to capture and document their expectations about your data.

It is time for your team to benefit from Great Expectations' automated testing that systematically surfaces errors, discrepancies and surprises lurking in your data, allowing you and your team to be more proactive when data changes.

We typically see two main deployment patterns that we will explore in depth below.

1. Great Expectations is **deployed adjacent to your existing data pipeline**.
2. Great Expectations is **embedded into your existing data pipeline**.



Responding to Validation Results
----------------------------------------

A :ref:`Validation Operator<validation_operators_and_actions>` is deployed at a particular point in your data pipeline.

A new batch of data arrives and the operator validates it against an expectation suite (see the previous step).

The :ref:`actions<actions>` of the operator store the validation result, add an HTML view of the result to the Data Docs website, and fire a configurable notification (by default, Slack).

If the data meets all the expectations in the suite, no action is required. This is the beauty of automated testing. No team members have to be interrupted.

In case the data violates some expectations, team members must get involved.

In the world of software testing, if a program does not pass a test, it usually means that the program is wrong and must be fixed.

In pipeline and data testing, if data does not meet expectations, the response to a failing test is triaged into 3 categories:

1. **The data is fine, and the validation result revealed a characteristic that the team was not aware of.**
  The team's data scientists or domain experts update the expectations to reflect this new discovery.
  They use the process described above in the Review and Edit sections to update the expectations while testing them against the data batch that failed validation.
2. **The data is "broken"**, and **can be recovered.**
  For example, the users table could have dates in an incorrect format.
  Data engineers update the pipeline code to deal with this brokenness and fix it on the fly.
3. **The data is "broken beyond repair".**
  The owners of the pipeline go upstream to the team (or external partner) who produced the data and address it with them.
  For example, columns in the users table could be missing entirely.
  The validation results in Data Docs makes it easy to communicate exactly what is broken, since it shows the expectation that was not met and observed examples of non-conforming data.
