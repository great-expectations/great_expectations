.. _supporting_resources:

Supporting Resources
=====================

Great Expectations requires a python compute environment and access to data, either locally or
through a database or distributed cluster. In addition, developing with great expectations relies
heavily on tools in the Python engineering ecosystem: pip, virtual environments, and jupyter notebooks.
We also assume some level of familiarity with git and version control.

See the links below for good, practical tutorials for these tools.

Tools Reference
==================

pip
-------------------------------------------

    * https://pip.pypa.io/en/stable/
    * https://www.datacamp.com/community/tutorials/pip-python-package-manager

virtual environments
-------------------------------------------

    * https://virtualenv.pypa.io/en/latest/
    * https://python-guide-cn.readthedocs.io/en/latest/dev/virtualenvs.html
    * https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/

jupyter notebooks and jupyter lab
-------------------------------------------

    * https://jupyter.org/
    * https://jupyterlab.readthedocs.io/en/stable/
    * https://towardsdatascience.com/jupyter-lab-evolution-of-the-jupyter-notebook-5297cacde6b

git
-------------------------------------------

    * https://git-scm.com/
    * https://reference.github.com/
    * https://www.atlassian.com/git/tutorials


Installing from github
===========================

If you plan to make changes to great expectations or live on the bleeding edge, you may want to clone from GitHub and \
pip install using the `--editable <https://stackoverflow.com/questions/35064426/when-would-the-e-editable-option-be-\
useful-with-pip-install>`__ flag.

.. code-block:: bash

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install -e great_expectations/

*last updated*: |lastupdate|
