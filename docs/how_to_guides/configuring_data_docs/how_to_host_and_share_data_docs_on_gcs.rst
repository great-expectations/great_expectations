.. _how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_gcs:

How to host and share Data Docs on GCS
======================================

This guide will explain how to host and share Data Docs on Google Cloud Storage.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <getting_started>`
    - `Set up a Google Cloud project <https://cloud.google.com/resource-manager/docs/creating-managing-projects>`_
    - `Set up the gsutil command line tool <https://cloud.google.com/storage/docs/gsutil_install>`_

Steps
-----

1. Create a Google Cloud Storage bucket using gsutil. Make sure you modify the project name, bucket name, and region for your situation.

  .. code-block:: bash
  
    > gsutil mb -p my_org_project -l US-EAST1 -b on gs://my_org_data_docs/
    Creating gs://my_org_data_docs/...

2. Save the bucket's current Cloud IAM policy.

  .. code-block:: bash

     gsutil iam get gs://my_org_data_docs/ > gcs-bucket-policy.json

3. Edit the ``gcs-bucket-policy.json`` file. Add a new binding with the role ``roles/storage.objectViewer``. Under the ``members`` key, list the members you would like to grant access to the bucket, using the form ``[MEMBER_TYPE]:[MEMBER_NAME]``. 

  .. code-block:: json
  
    {
      "bindings": [
        ...previously configured bindings 
        {
          "role": "roles/storage.objectViewer",
          "members": [
            "user:my_colleague@my_org.com",
            "domain:my_org.com"
          ]
        }
      ], 
      "etag": "CAI="
    }

4. Use gsutil to set the modified Cloud IAM policy on the bucket.

  .. code-block:: bash

    gsutil iam set gcs-bucket-policy.json gs://my_org_data_docs/

5. Add a new GCS site to the ``data_docs_sites`` section of your ``great_expectations.yml`` - you may also replace the default ``local_site``. 

  .. code-block:: yaml

    data_docs_sites:
      local_site:
        class_name: SiteBuilder
        show_how_to_buttons: true
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/
        site_index_builder:
          class_name: DefaultSiteIndexBuilder
      gs_site:  # this is a user-selected name - you may select your own
        class_name: SiteBuilder
        store_backend:
          class_name: TupleGCSStoreBackend
          project: my_org_project  # UPDATE the project name with your own 
          bucket: my_org_data_docs  # UPDATE the bucket name here to match the bucket you configured above
        site_index_builder:
          class_name: DefaultSiteIndexBuilder

6. Test that your configuration is correct by building the site using the following CLI command: ``great_expectations docs build --site-name gs_site``. If successful, the CLI will open your newly built GCS Data Docs site and provide the URL, which you can share as desired. Note that the URL will only be viewable by users you added to the above Cloud IAM policy. 

  .. code-block:: bash
  
    > great_expectations docs build --site-name gs_site
    
    The following Data Docs sites will be built:
    
     - gs_site: https://storage.googleapis.com/my_org_data_docs/index.html
    
    Would you like to proceed? [Y/n]: Y
    
    Building Data Docs...
    
    Done building Data Docs

Additional resources
--------------------

- `Overview of access control <https://cloud.google.com/storage/docs/access-control>`_
- `Using Cloud IAM permissions <https://cloud.google.com/storage/docs/access-control/using-iam-permissions>`_
- `Concepts related to identity (member types) <https://cloud.google.com/iam/docs/overview#concepts_related_identity>`_
- :ref:`Core concepts: Data Docs <data_docs>` 

Comments
--------

.. discourse::
   :topic_identifier: 232