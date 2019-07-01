.. _intro:

Introduction
==================

.. toctree::
   :maxdepth: 2

*Always know what to expect from your data.*

What is great\_expectations?
----------------------------

Great Expectations helps teams save time and promote analytic integrity by \
offering a unique approach to automated testing: pipeline tests. Pipeline \
tests are applied to data (instead of code) and at batch time (instead of \
compile or deploy time). Pipeline tests are like unit tests for datasets: \
they help you guard against upstream data changes and monitor data quality.

Software developers have long known that automated testing is essential for \
managing complex codebases. Great Expectations brings the same discipline, \
confidence, and acceleration to data science and engineering teams.

Why would I use Great Expectations?
-----------------------------------

To get more done with data, faster. Teams use great\_expectations to

-  Save time during data cleaning and munging.
-  Accelerate ETL and data normalization.
-  Streamline analyst-to-engineer handoffs.
-  Monitor data quality in production data pipelines and data products.
-  Simplify debugging data pipelines if (when) they break.
-  Codify assumptions used to build models when sharing with distributed
   teams or other analysts.

Workflow advantages
-------------------

Most data science and data engineering teams end up building some form of pipeline testing, eventually. Unfortunately, many teams don't get around to it until late in the game, long after early lessons from data exploration and model development have been forgotten.

In the meantime, data pipelines often become deep stacks of unverified assumptions. Mysterious (and sometimes embarrassing) bugs crop up more and more frequently. Resolving them requires painstaking exploration of upstream data, often leading to frustrating negotiations about data specs across teams.

It's not unusual to see data teams grind to a halt for weeks (or even months!) to pay down accumulated pipeline debt. This work is never fun---after all, it's just data cleaning: no new products shipped; no new insights kindled. Even worse, it's re-cleaning old data that you thought you'd already dealt with. In our experience, servicing pipeline debt is one of the biggest productivity and morale killers on data teams.

We strongly believe that most of this pain is avoidable. We built Great Expectations to make it very, very simple to

1. set up your testing framework early,
2. capture those early learnings while they're still fresh, and
3. systematically validate new data against them.

It's the best tool we know of for managing the complexity that inevitably grows within data pipelines. We hope it helps you as much as it's helped us.

Good night and good luck!


Use Cases
------------

TODO!!!




Great Expectations doesn't do X. Is it right for my use case?
-------------------------------------------------------------

It depends. If you have needs that the library doesn't meet yet, please
`upvote an existing
issue(s) <https://github.com/great-expectations/great_expectations/issues>`__
or `open a new
issue <https://github.com/great-expectations/great_expectations/issues/new>`__
and we'll see what we can do. Great Expectations is under active
development, so your use case might be supported soon.




