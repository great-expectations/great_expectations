.. _how_to_guides__configuring_datasources__how_to_configure_a_pandas_filesystem_datasource:

###############################################
How to configure a Pandas/filesystem Datasource
###############################################

This guide shows how to connect to a Pandas Datasource such that the data is accessible in the form of files on a local or NFS type of a filesystem.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

-----
Steps
-----

To add a filesystem-backed Pandas datasource do this:

#.
    Run ``great_expectations datasource new``
#.
    Choose the *Files on a filesystem (for processing with Pandas or Spark)* option from the menu (i.e., type ``1`` and press `ENTER`).
#.
    When asked *What are you processing your files with?*, choose ``Pandas`` (i.e., type ``1`` and press `ENTER`).
#.
    When prompted to *Enter the path (relative or absolute) of the root directory where the data files are stored.*, type in the ``/path/to/directory/containing/your/data/files`` and press `ENTER`.
#.
    When asked *Would you like to profile new Expectations for a single data asset within your new Datasource?*, press `ENTER` in order to continue.
#.
    Next, if you opt to *choose from a list of data assets in this datasource* (option ``1``), then select the number corresponding to your data file of interest from the list of your data files in the above directory, which will be displayed on your Terminal screen.
    Alternatively, you can choose option ``2`` and then type in the ``/path/to/your/data/file`` (including the file extension) and press `ENTER`.
#.
    Finally, if all goes well, some informational messages will appear on your Terminal screen.  After this, you can proceed with exploring the data sets in your new filesystem-backed Pandas data source.

----------------
Additional Notes
----------------

#.
    Relative path locations should be specified from the perspective of the directory, in which the ``great_expectations datasource new`` command is executed.

#.
    Note that if upon entering the path of the root directory where the data files are stored, you select *choose from a list of data assets in this datasource* (option ``1``), then only the root names of the available data files in that directory will be listed (i.e., without their file extensions).

--------
Comments
--------

    .. discourse::
        :topic_identifier: 167

