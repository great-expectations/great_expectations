.. _how_to_guides__configuring_datasources__how_to_configure_a_pandas_filesystem_datasource:

###############################################
How to configure a Pandas/filesystem Datasource
###############################################

This guide shows how to connect to a Pandas Datasource such that the data is accessible in the form of files on a local or NFS type of a filesystem.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

-----
Steps
-----

To add a filesystem-backed Pandas datasource do this:

#. **Run datasource new**

    From the command line, run:

    .. code-block:: bash

        great_expectations datasource new

#. **Choose "Files on a filesystem (for processing with Pandas or Spark)"**

    .. code-block:: bash

        What data would you like Great Expectations to connect to?
            1. Files on a filesystem (for processing with Pandas or Spark)
            2. Relational database (SQL)
        : 1

#. **Choose Pandas**

    .. code-block:: bash

        What are you processing your files with?
            1. Pandas
            2. PySpark
        : 1

#. **Specify the directory path for data files**

    .. code-block:: bash

        Enter the path (relative or absolute) of the root directory where the data files are stored.
        : /path/to/directory/containing/your/data/files

#. **Give your Datasource a name**

    When prompted, provide a custom name for your filesystem-backed Pandas data source, or hit Enter to accept the default.

    .. code-block:: bash

        Give your new Datasource a short name.
         [my_data_files_dir]:

    Great Expectations will now add a new Datasource 'my_data_files_dir' to your deployment, by adding this entry to your great_expectations.yml:

    .. code-block:: bash

          my_data_files_dir:
            data_asset_type:
              class_name: PandasDataset
              module_name: great_expectations.dataset
            batch_kwargs_generators:
              subdir_reader:
                class_name: SubdirReaderBatchKwargsGenerator
                base_directory: /path/to/directory/containing/your/data/files
            class_name: PandasDatasource

            Would you like to proceed? [Y/n]:

#. **Wait for confirmation**

    If all goes well, it will be followed by the message:

    .. code-block:: bash

        A new datasource 'my_data_files_dir' was added to your project.

    If you run into an error, you will see something like:

    .. code-block:: bash

        Error: Directory '/nonexistent/path/to/directory/containing/your/data/files' does not exist.

        Enter the path (relative or absolute) of the root directory where the data files are stored.
        :

    In this case, please check your data directory path, permissions, etc. and try again.

#.
    Finally, if all goes well and you receive a confirmation on your Terminal screen, you can proceed with exploring the data sets in your new filesystem-backed Pandas data source.

----------------
Additional Notes
----------------

#.
    Relative path locations should be specified from the perspective of the directory, in which the

    .. code-block:: bash

        great_expectations datasource new

    command is executed.

--------
Comments
--------

    .. discourse::
        :topic_identifier: 167

