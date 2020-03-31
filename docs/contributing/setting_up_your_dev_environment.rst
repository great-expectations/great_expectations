.. _contributing_setting_up_your_dev_environment:



Setting up your dev environment
==========================================

Prerequisites
-------------------

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
    * This will clone the ``develop`` branch of the great_expectations repo. Please use `develop` (not `master`!) as the starting point for your work.


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
    * Ex: ``virtualenv great_expectations_dev; source great_expectations_dev/bin/activate``
    * This is not required, but highly recommended.

**6. Install dependencies from requirements-dev.txt**

    * ``pip install -r requirements-dev.txt``
    *  This will ensure that sure you have the right libraries installed in your python environment.


**7. Install great_expectations from your cloned repo**

    * ``pip install -e .``
    * ``-e`` will install Great Expectations in "`editable <https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs>`__" mode. This is not required, but is often very convenient as a developer.

(Optional) Configure resources for testing and documentation
---------------------------------------------------------------

Depending on which features of Great Expectations you want to work on, you may want to configure different backends for local testing, such as postgresql and Spark. Also, there are a couple of extra steps if you want to build documentation locally.

**If you want to develop against local postgresql:**

    * #FIXME

**If you want to develop against local Spark:**

    * #FIXME
    * Probably (?): Make sure you have GCC and Java installed and working on your machine

**If you want to develop against a remote backend:**

    * #FIXME

**If you want to build documentation locally:**

    * ``pip install -r docs/requirements.txt``
    * To build documentation, the command is ``cd docs; make html``
    * Documentation will be generated in ``docs/build/html/`` with the ``index.html`` as the index page.

Run tests to confirm that everything is working
-----------------------------------------

You can run all tests by running ``pytest`` in the great_expectations directory root. Please see :ref:`contributing_testing` for testing options and details.

Start coding!
-----------------------------------------

At this point, you have everything you need to start coding!


*last updated*: |lastupdate|