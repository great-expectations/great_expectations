---
title: Using Data Connectors to divide Data Assets into Batches
---

To maintain their guarantees for the relationship between Batches, Data Connectors provide configuration options that
allow them to divide Data Assets into different Batches of data. We use the term "Data Reference" below to describe a
general pointer to data, like a filesystem path or database view. Batch Identifiers then define a conversion process:

1. Convert a Data Reference to a Batch Request, and
1. Convert a Batch Request back into a Data Reference (or Wildcard Data Reference, when searching)

## Configuring Data Connectors when dividing a Data Asset into Batches can be lossy

The main thing that makes dividing Data Assets into Batches complicated is that converting from a Batch Request to a
Data Reference can be lossy.

It’s pretty easy to construct examples where no regex can reasonably capture enough information to allow lossless
conversion from a Batch Request to a unique Data Reference:

### Example 1

For example, imagine a daily logfile that includes a random hash:

`YYYY/MM/DD/log-file-[random_hash].txt.gz`

The regex for this naming convention would be something like:

`(\d{4})/(\d{2})/(\d{2})/log-file-.*\.txt\.gz`

with capturing groups for YYYY, MM, and DD, and a non-capturing group for the random hash.

As a result, the Batch Identifiers keys will Y, M, D. Given specific Batch Identifiers:

```python
{
    "Y" : 2020,
    "M" : 10,
    "D" : 5
}
```

we can reconstruct *part* of the filename, but not the whole thing:

`2020/10/15/log-file-[????].txt.gz`

### Example 2

A slightly more subtle example: imagine a logfile that is generated daily at about the same time, but includes the exact
time stamp when the file was created.

`log-file-YYYYMMDD-HHMMSS.ssssssss.txt.gz`

The regex for this naming convention would be something like

`log-file-(\d{4})(\d{2})(\d{2})-.*\..*\.txt\.gz`

With capturing groups for YYYY, MM, and DD, but not the HHMMSS.sssssss part of the string. Again, we can only specify
part of the filename:

`log-file-20201015-??????.????????.txt.gz`

### Example 3

Finally, imagine an S3 bucket with log files like so:

`s3://some_bucket/YYYY/MM/DD/log_file_YYYYMMDD.txt.gz`

In that case, the user would probably specify regex capture groups with something
like `some_bucket/(\d{4})/(\d{2})/(\d{2})/log_file_\d+.txt.gz`.

The Wildcard Data Reference is how Great Expecations Data Connectors deal with that problem, making it easy to search
external stores and understand data.

Under the hood, when processing a Batch Request, the Data Connector may find multiple matching Batches. Generally, the
Data Connector will simply return a list of all matching Batch Identifiers.
