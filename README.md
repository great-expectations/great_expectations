[![Build Status](https://travis-ci.org/great-expectations/great_expectations.svg?branch=develop)](https://travis-ci.org/great-expectations/great_expectations)
[![Coverage Status](https://coveralls.io/repos/github/great-expectations/great_expectations/badge.svg?branch=develop)](https://coveralls.io/github/great-expectations/great_expectations?branch=develop)
[![Documentation Status](https://readthedocs.org/projects/great-expectations/badge/?version=latest)](http://great-expectations.readthedocs.io/en/latest/?badge=latest)

<img align="right" src="./generic_dickens_protagonist.png">

Great Expectations
================================================================================

*Always know what to expect from your data.*


What is great_expectations?
--------------------------------------------------------------------------------

Great Expectations is a framework that helps teams save time and promote analytic integrity with a new twist on automated testing: pipeline tests. Pipeline tests are applied to data (instead of code) and at batch time (instead of compile or deploy time).

Software developers have long known that automated testing is essential for managing complex codebases. Great Expectations brings the same discipline, confidence, and acceleration to data science and engineering teams.


Why would I use Great Expectations?
--------------------------------------------------------------------------------

To get more done with data, faster. Teams use great_expectations to

* Save time during data cleaning and munging.
* Accelerate ETL and data normalization.
* Streamline analyst-to-engineer handoffs.
* Monitor data quality in production data pipelines and data products.
* Simplify debugging data pipelines if (when) they break.
* Codify assumptions used to build models when sharing with distributed teams or other analysts.

How do I get started?
--------------------------------------------------------------------------------

It's easy! Just use pip install:


    $ pip install great_expectations

You can also clone the repository, which includes examples of using great_expectations.

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install great_expectations/

What expectations are available?
--------------------------------------------------------------------------------

Expectations include:
- `expect_table_row_count_to_equal`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_be_in_set`
- `expect_column_mean_to_be_between`
- ...and many more

Visit the [glossary of expectations](http://great-expectations.readthedocs.io/en/latest/glossary.html) for a complete list of expectations that are currently part of the great expectations vocabulary.

Can I contribute?
--------------------------------------------------------------------------------
Absolutely. Yes, please. Start [here](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING.md), and don't be shy with questions!


How do I learn more?
--------------------------------------------------------------------------------

For full documentation, visit [Great Expectations on readthedocs.io](http://great-expectations.readthedocs.io/en/latest/).

[Down with Pipeline Debt!](https://medium.com/@expectgreatdata/down-with-pipeline-debt-introducing-great-expectations-862ddc46782a) explains the core philosophy behind Great Expectations. Please give it a read, and clap, follow, and share while you're at it.

For quick, hands-on introductions to Great Expectations' key features, check out our walkthrough videos:

* [Introduction to Great Expectations](https://www.youtube.com/watch?v=-_0tG7ACNU4)
* [Using Distributional Expectations](https://www.youtube.com/watch?v=l3DYPVZAUmw&t=20s)


What's the best way to get in touch with the Great Expectations team?
--------------------------------------------------------------------------------

[Issues on GitHub](https://github.com/great-expectations/great_expectations/issues). If you have questions, comments, feature requests, etc., [opening an issue](https://github.com/great-expectations/great_expectations/issues/new) is definitely the best path forward.


Great Expectations doesn't do X. Is it right for my use case?
--------------------------------------------------------------------------------

It depends. If you have needs that the library doesn't meet yet, please [upvote an existing issue(s)](https://github.com/great-expectations/great_expectations/issues) or [open a new issue](https://github.com/great-expectations/great_expectations/issues/new) and we'll see what we can do. Great Expectations is under active development, so your use case might be supported soon.
