.. _contributing_setting_up_your_dev_environment:



Setting up your dev environment
===============================

This Guide will walk you through setting up your environment to contribute to Great Expectations.

You can also watch steps 1-7 of this guide as a `video <https://www.youtube.com/watch?v=sps0C1fblu4>`__.

Prerequisites
-------------

In order to contribute to Great Expectations, you will need the following:

* A GitHub account---this is sufficient if you :ref:`only want to contribute to the documentation <contributing_make_changes_through_github>`.
* If you want to contribute code, you will also need a working version of Git on your computer. Please refer to the `Git setup instructions <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`__ for your environment.
* We also recommend going through the `SSH key setup process on GitHub <https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent>`__ for easier authentication.


Fork and clone the repository
------------------------------

**1. Fork the Great Expectations repo**

    * Go to the `Great Expectations repo on GitHub <https://github.com/great-expectations/great_expectations>`__.
    * Click the ``Fork`` button in the top right. This will make a copy of the repo in your own GitHub account.
    * GitHub will take you to your forked version of the repository.


**2.  Clone your fork**

    * Click the green ``Clone`` button and choose the SSH or HTTPS URL depending on your setup.
    * Copy the URL and run ``git clone <url>`` in your local terminal.
    * This will clone the ``develop`` branch of the great_expectations repo. Please use ``develop`` (not ``main``!) as the starting point for your work.
    * Atlassian has a `nice tutorial for developing on a fork <https://www.atlassian.com/git/tutorials/git-forks-and-upstreams>`__.


**3. Add the upstream remote**

    * On your local machine, cd into the ``great_expectations`` repo you cloned in the previous step.
    * Run: ``git remote add upstream git@github.com:great-expectations/great_expectations.git``
    * This sets up a remote called ``upstream`` to track changes to the main branch.

**4. Create a feature branch to start working on your changes.**

    * Ex: ``git checkout -b feature/my-feature-name``
    * We do not currently follow a strict naming convention for branches. Please pick something clear and self-explanatory, so that it will be easy for others to get the gist of your work.


Install python dependencies
------------------------------

**5. Create a new virtual environment**

    * Make a new virtual environment (e.g. using virtualenv or conda), name it "great_expectations_dev" or similar.
    * Ex virtualenv: ``python3 -m venv <path_to_environments_folder>/great_expectations_dev`` and then ``<source path_to_environments_folder>/great_expectations_dev/bin/activate``
    * Ex conda: ``conda create --name great_expectations_dev python=3.7`` and then ``conda activate great_expectations_dev`` (we support multiple python versions, you may select something other than 3.7).
    * This is not required, but highly recommended.

**6. Install dependencies from requirements-dev.txt**

    * ``pip install -r requirements-dev.txt -c constraints-dev.txt``
    *  MacOS users will be able to pip / pip3 install ``requirements-dev.txt`` using the above command from within conda, yet Windows users utilizing a conda environment will need to individually install all files within ``requirements-dev.txt``
    *  This will ensure that sure you have the right libraries installed in your python environment.
    
      * Note that you can also substitute ``requirements-dev-test.txt`` to only install requirements required for testing all backends, and ``requirements-dev-spark.txt`` or ``requirements-dev-sqlalchemy.txt`` if you would like to add support for spark or sqlalchemy tests, respectively. For some database backends, such as MSSQL additional driver installation may required in your environment; see below for more information.
      * For some users, installation of certain Pyspark versions (such as 2.4.7) may cause import errors. To fix this simply run ``pip install pyspark --upgrade`` to upgrade to the latest version, which should fix all import errors.
      * `Installing Microsoft ODBC driver for MacOS <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos>`__
      * `Installing Microsoft ODBC driver for Linux <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>`__


**7. Install great_expectations from your cloned repo**

    * ``pip install -e .``
    * ``-e`` will install Great Expectations in "`editable <https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs>`__" mode. This is not required, but is often very convenient as a developer.

(Optional) Configure resources for testing and documentation
---------------------------------------------------------------

Depending on which features of Great Expectations you want to work on, you may want to configure different backends for local testing, such as postgresql and Spark. Also, there are a couple of extra steps if you want to build documentation locally.

**If you want to develop against local postgresql:**

    * To simplify setup, the repository includes a docker-compose file that can stand up a local postgresql container. To use it, you'll need to have `docker installed <https://docs.docker.com/install/>`__.
    * Navigate to ``assets/docker/postgresql`` in  your ``great_expectations`` repo and run ``docker-compose up -d``
    * Within the same directory, you can run ``docker-compose ps`` to verify that the container is running. You should see something like:

        .. code-block::

                    Name                       Command              State           Ports         
            ———————————————————————————————————————————
            postgresql_travis_db_1   docker-entrypoint.sh postgres   Up      0.0.0.0:5432->5432/tcp

..

    * Once you’re done testing, you can shut down your postgesql container by running ``docker-compose down`` from the same directory.
    * Caution: If another service is using port 5432, docker may start the container but silently fail to set up the port. In that case, you will probably see errors like this:

        .. code-block::

            psycopg2.OperationalError: could not connect to server: Connection refused
                Is the server running on host "localhost" (::1) and accepting
                TCP/IP connections on port 5432?
            could not connect to server: Connection refused
                Is the server running on host "localhost" (127.0.0.1) and accepting
                TCP/IP connections on port 5432?
        
    * Or this...

        .. code-block::

            sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) FATAL:  database "test_ci" does not exist
            (Background on this error at: http://sqlalche.me/e/e3q8)


**If you want to develop against local mysql:**

    * To simplify setup, the repository includes a docker-compose file that can stand up a local mysqldb container. To use it, you'll need to have `docker installed <https://docs.docker.com/install/>`__.
    * Navigate to ``assets/docker/mysql`` in  your ``great_expectations`` repo and run ``docker-compose up -d``
    * Within the same directory, you can run ``docker-compose ps`` to verify that the container is running. You should see something like:

        .. code-block::

                  Name                   Command             State                 Ports
            ------------------------------------------------------------------------------------------
            mysql_mysql_db_1   docker-entrypoint.sh mysqld   Up      0.0.0.0:3306->3306/tcp, 33060/tcp

..

    * Once you’re done testing, you can shut down your mysql container by running ``docker-compose down`` from the same directory.
    * Caution: If another service is using port 3306, docker may start the container but silently fail to set up the port.

**If you want to develop against local Spark:**

    * In most cases, ``pip install requirements-dev.txt`` should set up pyspark for you.
    * If you don't have Java installed, you will probably need to install it and set your ``PATH`` or ``JAVA_HOME`` environment variables appropriately.
    * You can find official installation instructions for spark `here <https://spark.apache.org/docs/latest/index.html#downloading>`__.

**If you want to build documentation locally:**

    * ``pip install -r docs/requirements.txt``
    * To build documentation, the command is ``cd docs; make html``
    * Documentation will be generated in ``docs/build/html/`` with the ``index.html`` as the index page.
    * Note: we use ``autoapi`` to generate API reference docs, but it's not compatible with pandas 1.1.0. You'll need to have pandas 1.0.5 (or a previous version) installed in order to successfully build docs.

Run tests to confirm that everything is working
-----------------------------------------------

You can run all tests by running ``pytest`` in the great_expectations directory root. Please see :ref:`contributing_testing` for testing options and details.

Start coding!
-----------------------------------------

At this point, you have everything you need to start coding!
