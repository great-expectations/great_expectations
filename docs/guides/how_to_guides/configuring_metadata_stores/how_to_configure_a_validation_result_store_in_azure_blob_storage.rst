.. _how_to_guides__configuring_metadata_stores__how_to_configure_a_validation_result_store_in_azure_blob_storage:

.. warning:: This doc is a stub.

How to configure a Validation Result store in Azure blob storage
================================================================

By default, Validations are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.

This feature has been implemented in the :py:class:`great_expectations.data_context.store.tuple_store_backend.TupleAzureBlobStoreBackend` class. We do not have a how-to guide for it yet. Please follow the instructions in the other how-to guides and in the doc strings for the class now.

**If you want to be a real hero, we'd welcome a contribution for this how-to guide.** Please see :ref:`the Contributing tutorial <contributing>` and :ref:`how_to_guides__miscellaneous__how_to_write_a_how_to_guide` to get started.

.. discourse::
    :topic_identifier: 173
