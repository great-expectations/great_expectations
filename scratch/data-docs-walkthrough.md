# Data Docs Walkthrough Prototype



An expectation is a falsifiable, verifiable statement about data.

Expectations provide a language to talk about data characteristics and data quality - humans-humans, humans-machines and machines-machines.

Expectations are both tests of data and its documentation.

This is an expectation:
![](data-docs-walkthrough/values_not_null_html_en.jpg)

It can be presented as a human-friendly HTML.

Also, in other languages:
![](data-docs-walkthrough/values_not_null_html_de.jpg)

An expectation can be presented in a machine-friendly JSON:
![](data-docs-walkthrough/values_not_null_json.jpg)

A machine can test if a dataset conforms to the expectation. 

Validation produces a validation result object:
![](data-docs-walkthrough/values_not_null_validation_result_json.jpg)

This object has rich context about the test failure.

The validation result can be rendered in HTML:
![](data-docs-walkthrough/values_not_null_validation_result_html_en.jpg)

Data team members can use this HTML to communicate about test failures.


![](data-docs-walkthrough/home.png)
This is the Data Docs home page!

![](data-docs-walkthrough/home_tables.jpeg)
These are the chunks of data that you have told Great Expectations about.

![](data-docs-walkthrough/home_suites.jpeg)
These are the Expectations Suites.

![](data-docs-walkthrough/home_validation_results_succeeded.jpeg)
These are validation results that passed.

![](data-docs-walkthrough/suite_overview.png)
This is an overview of an Expectation Suite

![](data-docs-walkthrough/suite_toc.jpeg)
This lists all the fields in your data.

![](data-docs-walkthrough/home_validation_results_failed.jpeg)
These are validation results that failed.

![](data-docs-walkthrough/validation_overview.png)
This is the overview of a Validation.

![](data-docs-walkthrough/validation_passed.jpeg)
This is a single passed Expectation. Note the observed value.

![](data-docs-walkthrough/validation_failed.jpeg)
This is a single failed Expectation. Note the observed value.

![](data-docs-walkthrough/validation_failed_unexpected_values.jpeg)
This is a single failed Expectation. Note the observed value includes unexpected values from your data. This helps you debug pipeines faster.

![](data-docs-walkthrough/validation_failed_unexpected_values.gif)
This is a single failed Expectation. Note the observed value includes unexpected values from your data. This helps you debug pipeines faster.
