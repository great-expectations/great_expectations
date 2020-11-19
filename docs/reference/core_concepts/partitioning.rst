
#############################################
Partitioners that plug into FileDataConnector
#############################################

Each Partitioner must support two primary operations:

	1. Convert a DataReference to a BatchRequest, and
	2. Convert a BatchRequest back into DataReference (or WildcardDataReference)

* ``DataReferences`` are just strings that uniquely identify a file, S3 object, etc. that the DataConnector could connect to.
* ``WildcardDataReferences`` are similar, but they can contain wildcards. In most uses, we *hope* that they uniquely identify a single file, but we cannot guarantee it.

***********************************
Lossy conversion, in three examples
***********************************

The main thing that makes Partitioners complicated is that converting from a BatchRequest to a DataReference can be lossy.

It’s pretty easy to construct examples where no regex can reasonably capture enough information to allow lossless conversion from a BatchRequest to a unique DataReference:

*********
Example 1
*********

For example, imagine a daily logfile that includes a random hash.

	``YYYY/MM/DD/log-file-[random_hash].txt.gz``

The regex for this naming convention would be something like

	``(\d{4})/(\d{2})/(\d{2})/log-file-.*\.txt\.gz``

with capturing groups for YYYY, MM, and DD, and a non-capturing groups for the random hash.

As a result, the PartitionDefinition keys will Y, M, D. Given a specific PartitionDefinition:

.. code-block:: python

    {
        "Y" : 2020,
        "M" : 10,
        "D" : 5
    }


we can reconstruct *part* of the filename, but not the whole thing:

	``2020/10/15/log-file-[????].txt.gz``

*Example 2*

A slightly more subtle example: imagine a logfile that is generated daily at about the same time, but includes the exact time stamp when the file was created.

	``log-file-YYYYMMDD-HHMMSS.ssssssss.txt.gz``

The regex for this naming convention would be something like

	``log-file-(\d{4})(\d{2})(\d{2})-.*\..*\.txt\.gz``

With capturing groups for YYYY, MM, and DD, but not the HHMMSS.sssssss part of the string. Again, we can only specify part of the filename:

	``log-file-20201015-??????.????????.txt.gz``

*Example 3*

Finally, imagine an S3 bucket with log files like so:

    ``s3://some_bucket/YYYY/MM/DD/log_file_YYYYMMDD.txt.gz``

In that case, the user would probably specify regex capture groups something like ``some_bucket/(\d{4})/(\d{2})/(\d{2})/log_file_\d+.txt.gz``

Note that we use a wildcard for the final YYYMMDD, but it’s not in a capturing group. In this case, it’s theoretically possible to reconstruct the whole filename, but the regex group specified by the user doesn’t give us enough information to do so.

One option is to ask users to try to capture all the elements of the group, then define which ones should be treated as keys and which keys are duplicated in more than one place. This seems difficult and error-prone. A lot to ask of our users.

To wit: there are many naming conventions where you can always go from a DataReference to a unique BatchDefinition without loss, but the reverse is not true. We need to deal with the fairly common case of lossy BatchDefinition-to-DataReference conversion.

*****************************
Dealing with lossy conversion
*****************************

This is a solvable problem, but it requires a little bit of extra machinery.

Proposal: Implement two primary methods for `RegexPartitioner`

.. code-block:: python

    def convert_data_reference_to_batch_request(
      data_reference: DataReference
    ) -> BatchRequest :
    	pass

This one’s easy. All it takes is extracting groups from the regex and bundling them into a ``BatchRequest``.

.. code-block:: python

    def convert_batch_request_to_data_reference(
        batch_request: BatchRequest
    ) -> DataReference or WilcardDataReference:


This one is a little harder. In the case where the Partitioner can fully specify the file, this method will return a DataReference. If not, it will return a WildcardDataReference.

The DataConnector will handle the results as follows:

.. code-block:: python

    possibly_wildcard_data_reference = convert_batch_request_to_data_reference(batch_request)

    matching_objects = query_file_store_to_list_matching_objects(possibly_wildcard_data_reference)

    if len(matching_objects) == 0:
        raise ValueError("No objects in {file_store} matched data reference {possibly_wildcard_data_reference} generated from BatchRequest {batch_request}")

    elif len(matching_objects) > 1:
        # Assuming we're not allowing grouping or multiple batches...
        raise ValueError("Multiple objects in {file_store} matched data reference {possibly_wildcard_data_reference} generated from BatchRequest {batch_request}: {matching_objects}")

    else:
        # Extract the relevant matching_objects[0]


**********************
Differences by backend
**********************

Aside from complexity from lossy conversion, we’re going to need to deal with differences among backends.

There are 3 primary differences among backends for object-store-type DataConnectors:

    - Different backends provide different APIs for listing, fetching, and paginating results. Wildcards are a good example.
	- Different backends have different SDKs and
	- Different backends require different credentials for access

I propose that we deal with this simply by subclassing FileDataConnector with S3DataConnector, GCSDataConnector, AzureBlobStoreDataConnector, etc.

Note: we may want to implement a less-general-but-easier-to-configure version of FileDataConnector (e.g. ``SinglePartitionerFileDataStore``). In that case, subclassing different backends may create diamond inheritance among classes. That’s probably okay…? I doubt the inherited methods will conflict.

Question: Aside from RegexPartitioner, are there other Partitioners that are compatible with FileDataConnector (and its subclasses)?
    - If not, should we consider making the RegexPartitioner part of the FileDataConnector class?
