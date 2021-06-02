.. _how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_a_filesystem:

How to host and share Data Docs on a filesystem
================================================

This guide will explain how to host and share Data Docs on a filesystem.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations. <tutorials__getting_started>`

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. **Filesystem-hosted Data Docs are configured by default for Great Expectations deployments created using great_expectations init.**

          To create additional Data Docs sites, you may re-use the default Data Docs configuration below. You may replace ``local_site`` with your own site name, or leave the default.

          .. code-block:: yaml

            data_docs_sites:
              local_site:  # this is a user-selected name - you may select your own
                class_name: SiteBuilder
                store_backend:
                  class_name: TupleFilesystemStoreBackend
                  base_directory: uncommitted/data_docs/local_site/ # this is the default path but can be changed as required
                site_index_builder:
                  class_name: DefaultSiteIndexBuilder

        2. **Test that your configuration is correct by building the site.**

            Use the following CLI command: ``great_expectations docs build --site-name local_site``. If successful, the CLI will open your newly built Data Docs site and provide the path to the index page.

            .. code-block:: bash

                > great_expectations docs build --site-name local_site

                The following Data Docs sites will be built:

                 - local_site: file:///great_expectations/uncommitted/data_docs/local_site/index.html

                Would you like to proceed? [Y/n]: Y

                Building Data Docs...

                Done building Data Docs


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. **Filesystem-hosted Data Docs are configured by default for Great Expectations deployments created using great_expectations init.**

          To create additional Data Docs sites, you may re-use the default Data Docs configuration below. You may replace ``local_site`` with your own site name, or leave the default.

          .. code-block:: yaml

            data_docs_sites:
              local_site:  # this is a user-selected name - you may select your own
                class_name: SiteBuilder
                store_backend:
                  class_name: TupleFilesystemStoreBackend
                  base_directory: uncommitted/data_docs/local_site/ # this is the default path but can be changed as required
                site_index_builder:
                  class_name: DefaultSiteIndexBuilder

        2. **Test that your configuration is correct by building the site.**

            Use the following CLI command: ``great_expectations --v3-api docs build --site-name local_site``. If successful, the CLI will open your newly built Data Docs site and provide the path to the index page.

            .. code-block:: bash

                > great_expectations --v3-api docs build --site-name local_site

                The following Data Docs sites will be built:

                 - local_site: file:///great_expectations/uncommitted/data_docs/local_site/index.html

                Would you like to proceed? [Y/n]: Y

                Building Data Docs...

                Done building Data Docs

Additional notes
----------------

- To share the site, you can zip the directory specified under the ``base_directory`` key in your site configuration and distribute as desired.

Additional resources
--------------------

- :ref:`Core concepts: Data Docs <data_docs>`

Comments
--------

  .. discourse::
     :topic_identifier: 230
