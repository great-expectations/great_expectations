[![Build Status](https://travis-ci.org/great-expectations/great_expectations.svg?branch=develop)](https://travis-ci.org/great-expectations/great_expectations)
[![Coverage Status](https://coveralls.io/repos/github/great-expectations/great_expectations/badge.svg?branch=develop)](https://coveralls.io/github/great-expectations/great_expectations?branch=develop)
[![Documentation Status](https://readthedocs.org/projects/great-expectations/badge/?version=latest)](http://great-expectations.readthedocs.io/en/latest/?badge=latest)

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


Getting started
--------------------------------------------------------------------------------

...is easy. Just use pip install:

.. code-block:: bash

    $ pip install great_expectations

You can also clone the repository, which includes examples of using great_expectations.

.. code-block:: bash

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install great_expectations/

Since Great Expectation is under active development, the `develop` branch is often a ahead of the latest production release. If you want to work from the latest commit on `develop`, we recommend you install by branch name or hash.

branch-name:

.. code-block:: bash

	$ pip install git+git://github.com/great-expectations/great_expectations.git@develop

Visit [the Great Expectations documentation](http://great-expectations.readthedocs.io/en/latest/) for more info.