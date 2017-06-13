great_expectations
================================================================================


Always know what to expect from your data.

--------------------------------------------------------------------------------

What is great_expectations?
--------------------------------------------------------------------------------

great_expectations is a python framework for bringing data pipelines and products under test.

Software developers have long known that automated testing is essential for managing complex codebases. Using the concept of [pipeline testing](), great_expectations brings the same discipline, confidence, and acceleration to data science and engineering teams.


Why would I use great_expectations?
--------------------------------------------------------------------------------

To get more done with data, faster. Teams use great_expectations to

* Save time during data cleaning and munging.
* Accelerate ETL and data normalization.
* Streamline analyst-to-engineer handoffs.
* Monitor data quality in production data pipelines and data products.
* Simplify debugging data pipelines if (when) they break.


See [workflow advantages]() to learn more about how great_expectations speeds up data teams.


Getting started
--------------------------------------------------------------------------------

... in great_expectations is dead simple. To install:

```
    $ git clone https://github.com/abegong/great_expectations.git
    $ cd great_expectations
    $ python setup.py install
```

To add pipeline tests to a project:

```
    $ cd my_project
    $ great_expectations initialize
```

great_expectations will prompt you to add data sources, then scaffold pipeline testing files for you. The framework knows how to connect to most common structured and semi-structured data sources, including:

* Relational data stores (MySQL, Postgresql, OracleDB, SQLServer, Redshift)
* Flat files (csv, delimited text files, excel, xml, json-lines)
* Flat file directories (on local paths, URLs, S3)

Expectations
--------------------------------------------------------------------------------

Expectations are the workhorse abstraction in great_expectations. Like assertions in traditional python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, great_expectations applies Expectations to data instead of code. (We find that this makes intuitive sense to many data scientists and engineers. See [pipeline testing]() for a more detailed explanation.)

great_expectations's connect-and-expect API makes it easy to declare Expectations within the tools you already use for data exploration: jupyter notebooks, the ipython console, scratch scripts, etc.

```
    >> import great_expectations as ge
    >> ge.list_sources()
    ['our_postgres_db', 'last_minute_inventory.xlsx',]

    >> our_postgres_db = ge.connect_source('our_postgres_db')
    >> our_postgres_db.list_tables()
    ['users', 'inventory', 'discoveries']


    >> # Connect to a specific Table
    >> users_table = our_postgres_db.users
    >>
    >> # Create a new Expectation
    >> users_table.user_id.expect_column_values_to_be_unique()
    >>
    >> # Save the Expectation to great_expectations/my_postgres_users_table.json
    >> users_table.save_expectations()
```

When you invoke an Expectation method from a notebook or console, it will immediately return a dictionary containing the result and information about any execptions

For example:

```
    >> users_table.user_id.list()
    [3,5,4,6,9,7,8,0,2,10,11,12,13,14,15,1,16,17,18,19,20,21,26,27,28,29,22,23,24,25]

    >> users_table.user_id.expect_column_values_to_be_unique()
    {
        "success" : True,
        "exception_list" : []
    }
```

Another example:

```
    >> discoveries_table.discoverer_first_name.expect_column_values_to_be_in_set(['Edison', 'Bell'])
    {
        "success" : False,
        "exception_list" : ["Curie", "Curie"]
    }

    >> discoveries_table.discoverer_first_name.expect_column_values_to_be_in_set([
        'Edison', 'Bell', 'Curie'
       ])
    {
        "success" : True,
        "exception_list" : []
    }
```

This instant feedback helps you zero in on exceptions very quickly, taking a lot of the pain and guesswork out of early data exploration.

great_expectations's Expectations have been developed by a broad cross-section of data scientists and engineers. Check out the [full list of Expectations](); it covers all kinds of practical use cases:

* Foreign key verification and row-based accounting for ETL
* Form validation and regex pattern-matching for names, URLs, dates, addresses, etc.
* Checks for missing data
* Crosstabs
* Distributions for statistical modeling. 
* etc.

At the end of your exploration, call `save_expectations` to store all Expectations from your session to your pipeline test files. (See [under the hood]() for a more detailed explanation of how this all works.)

This is how you always know what to expect from your data.

```
    >> our_postgres_db.save_expectations()
```

Validation
--------------------------------------------------------------------------------

Once you've constructed Expectations, you can use them to validate new data.

```
    >> import great_expectations as ge
    >> users_table = ge.connect_to_table('our_postgres_db', 'users')
    >> users_table.validate()
    user_id    expect_column_values_to_be_unique : True, []
```

Calling great_expectations's validation method generates a JSON-formatted report describing the outcome of all expectations.

```
    >> discoveries_table = ge.connect_to_table('our_postgres_db', 'discoveries')
    >> discoveries_table.validate()
    {
        "expectation_type" : "expect_column_values_to_be_in_set",
        "column" : "discoverer_first_name",
        "values" : ["Edison", "Bell"],
        "success" : false,
        "exception_list" : ["Curie", "Curie"]
    }
```

This is especially powerful when combined with great_expectations's command line tool, which lets you validate in a one-line bash script. You can validate a single Table:

    $ great_expectations validate our_postgres_db.users

...or a whole data Source...

    $ great_expectations validate our_postgres_db

...or the entire project.

    $ great_expectations validate

Useful deployment patterns include

* Include validation at the end of a complex data transformation, to verify that no cases were lost, duplicated, or improperly merged
* Include validation at the *beginning* of a script applying a machine learning model to a new batch of data, to verify that its distributed similarly to the training and testing set.
* Automatically trigger table-level validation when new data is dropped to an FTP site or S3 bucket, and send the validation report to the uploader and bucket owner by email .
* Schedule database validation jobs using cron, then capture errors and warnings (if any) and post them to Slack.
* Validate as part of an Airflow task: if Expectations are violated, raise an error and stop DAG propagation until the problem is resolved. Alternatively, you can implement expectations that raise warnings without halting the DAG.


Closing remarks
--------------------------------------------------------------------------------
Most data science and data engineering teams end up building some form of pipeline testing, eventually. Unfortunately, many teams don't get around to it until late in the game, long after early lessons from data exploration and model development have been forgotten.

In the meantime, data pipelines often become deep stacks of unverified assumptions. Mysterious (and sometimes embarrassing) bugs crop up more and more frequently. Resolving them requires painstaking exploration of upstream data, often leading to frustrating negotiations about data specs across teams.

It's not unusual to see data teams grind to a halt for weeks (or even months!) to pay down accumulated pipeline debt. This work is never fun---after all, it's just data cleaning: no new products shipped; no new insights kindled. Even worse, it's re-cleaning old data that you thought you'd already dealt with. In my experience, servicing pipeline debt is one of the biggest productivity and morale killers on data teams.

We strongly believe that most of this pain is avoidable. We built great_expectations to make it very, very simple to

1. set up your testing framework early,
2. capture those early learnings while they're still fresh, and
3. systematically validate new data against them.

It's the best tool we know of for managing the complexity that inevitably grows within data pipelines. We hope it helps you as much as it's helped us.

Good night and good luck!


