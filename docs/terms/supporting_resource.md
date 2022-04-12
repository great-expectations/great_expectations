---
id: supporting_resource
title: Supporting Resources
hoverText: A resource external to the Great Expectations code base which Great Expectations utilizes.
---

import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';

<UniversalMap setup='active' connect='active' create='active' validate='active'/> 

## Overview

### Definition

A Supporting Resource is a resource external to the Great Expectations code base which Great Expectations utilizes.

### Features and promises

Great Expectations requires a Python compute environment and access to data, either locally or through a database or distributed cluster. In addition, developing with Great Expectations relies heavily on tools in the Python engineering ecosystem: pip, virtual environments, and Jupyter Notebooks. We also assume some level of familiarity with Git and version control.

See the links under the [external documentation section](#external-documentation) below for good, practical tutorials for these tools.

## Use cases

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

Supporting Resources are used throughout Great Expectations workflows.  Some are necessities for Great Expectations to operate.  Others provide convenience when performing certain tasks, and some are recommended for project security and stability.  Below you will find an outline of the major Supporting Resources, and a brief description of how and when they are used.

- **Source Data Systems:** Without data to Validate, Great Expectations has no purpose.  That data can exist natively in a variety of source data systems, including filesystems, databases, and distributed clusters.
- **Python:** Great Expectations is built in Python.  The objects that you create and methods that you use while working with Great Expectations will be generally be Python objects, YAML files, or JSON files.  Great Expectations cannot run without a Python environment.
- **pip:** Pip is the standard package manager for Python.  You will use pip to install Great Expectations in your Python environment.
- **Virtual Environments:** Virtual environments are isolated environments for projects.  The recommended best practice for using Great Expectations is to install it into a Python Virtual Environment to ensure that your Great Expectations deployment is not impacted by any other projects you may be working on, and vice versa.
- **Jupyter Notebooks:** A Jupyter Notebook is a web-based interactive computing platform.  A Jupyter Notebook can combine live code, explanatory text, and visualizations into a single location.  Great Expectations' CLI frequently uses Jupyter Notebooks to provide you with code boilerplate alongside additional explanations and guidance to assist you in various tasks.
- **Git and version control:** Version control is an important part of any project.  It allows you to track changes in your files and easily rollback to earlier versions of them if needed.  Best practices for Great Expectations  is to maintain your Data Context under version control.


## External documentation

If you need to know more about a Supporting Resource, it is best to reference that resource's documentation.  Below are links to tutorials that can get you started with these tools.

### pip
* [https://pip.pypa.io/en/stable/](https://pip.pypa.io/en/stable/)
* [https://www.datacamp.com/community/tutorials/pip-python-package-manager](https://www.datacamp.com/community/tutorials/pip-python-package-manager)

### Virtual Environments
* [https://virtualenv.pypa.io/en/latest/](https://virtualenv.pypa.io/en/latest/)
* [https://python-guide-cn.readthedocs.io/en/latest/dev/virtualenvs.html](https://python-guide-cn.readthedocs.io/en/latest/dev/virtualenvs.html)
* [https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/](https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/)
  
### Jupyter Notebooks and Jupyter Lab
* [https://jupyter.org/](https://jupyter.org/)
* [https://jupyterlab.readthedocs.io/en/stable/](https://jupyterlab.readthedocs.io/en/stable/)
* [https://towardsdatascience.com/jupyter-lab-evolution-of-the-jupyter-notebook-5297cacde6b](https://towardsdatascience.com/jupyter-lab-evolution-of-the-jupyter-notebook-5297cacde6b)

### Git
* [https://git-scm.com/](https://git-scm.com/)
* [https://www.atlassian.com/git/tutorials](https://www.atlassian.com/git/tutorials)







