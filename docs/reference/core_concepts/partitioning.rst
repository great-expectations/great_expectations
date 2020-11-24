
#############################################
Partitioning Data Assets in Data Connectors
#############################################

To maintain their guarantees for the relationship between Batches, Data Connectors provide configuration options that allow them to partition data assets. We use the term "DataReference" below to describe a general pointer to data, like a filesystem path or database view. Partition Definitions then define a conversion process:

	1. Convert a DataReference to a BatchRequest, and
	2. Convert a BatchRequest back into DataReference (or WildcardDataReference, when searching)

**********************************************************************
Configuring Data Connectors when Partitioning may be Lossy
**********************************************************************


The main thing that makes Partitioning complicated is that converting from a BatchRequest to a DataReference can be lossy.

It’s pretty easy to construct examples where no regex can reasonably capture enough information to allow lossless conversion from a BatchRequest to a unique DataReference:

Example 1
------------

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

Example 2
------------


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

The WildcardDataReference is how Great Expecations Data Connectors deal with that problem, making it easy to search external stores and understand data.

Under the hood, when processing a batch request, the Data Connector may find multiple matching partitions. The DataConnector will generally simply return a list of all matching PartitionDefinition objects.
