.. _how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_in_amazon_s3:

How to configure an Expectation store in Amazon S3
==================================================

By default, newly profiled Expectations are stored in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  This guide will help you configure Great Expectations to store them in an Amazon S3 bucket.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured `boto3 <https://github.com/boto/boto3>`_ to connect to the Amazon S3 bucket where Expectations will be stored.

Steps
-----
1. Look for the following lines in the ``great_expectations.yml`` file.

    .. code-block:: yaml

        expectations_store_name: expectations_store

        stores:
            expectations_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: expectations/

The configuration file tells Great Expectations to look for Expectations in a store called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.


2. Update your configuration file to include a new store for Expectations on S3.  In our case the, name is set to ``expectations_S3_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleS3StoreBackend``, ``bucket`` will be set to the address of your S3 bucket, and ``prefix`` will be set to the folder where Expectation files are located.  If you are also storing :ref:`Validations in S3, <how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_s3>` please ensure that the ``prefix`` values are disjoint and one is not a substring of the other

    .. code-block:: yaml

        expectations_store_name: expectations_S3_store

        stores:
            expectations_S3_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: TupleS3StoreBackend
                    bucket: '<your_s3_bucket_name>'
                    prefix: '<your_s3_bucket_folder_name>'

3. Confirm that the S3 Expectations store has been added by running ``great_expectations store list``. Notice the output contains two Expectation stores: the original ``expectations_store`` on the local filesystem and the ``expectations_S3_store`` we just configured.  This is ok, since Great Expectations will look for Expectations in the S3 bucket as long as we set the ``expectations_name`` variable to ``expectations_S3_store``.

    .. code-block:: bash

        great_expectations store list

        - name: expectations_store
        class_name: ExpectationsStore
        store_backend:
            class_name: TupleFilesystemStoreBackend
            base_directory: expectations/

        - name: expectations_S3_store
        class_name: ExpectationsStore
        store_backend:
            class_name: TupleS3StoreBackend
            bucket: '<your_s3_bucket_name>'
            prefix: '<your_s3_bucket_folder_name>'


4. Confirm that the Validation store has been updated by running a :ref:`Checkpoint <tutorials__getting_started__set_up_your_first_checkpoint>`, or visualize the results by re-building :ref:`Data Docs <tutorials__getting_started__set_up_data_docs>`.


Additional resources
--------------------

- Instructions on how to set up `boto3 <https://github.com/boto/boto3>`_ with AWS can be found at boto3's `documentation site <https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>`_.

If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.

.. discourse::
    :topic_identifier: 178

