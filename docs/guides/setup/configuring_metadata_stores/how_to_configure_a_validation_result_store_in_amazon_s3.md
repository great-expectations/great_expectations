---
title: How to configure a Validation Result store in Amazon S3
---

import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '/docs/term_tags/_tag.mdx';

By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  Since Validations may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system. This guide will help you configure a new storage location for Validations in Amazon S3.

<Prerequisites>

- Configured a [Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- Configured an [Expectations Suite](../../../tutorials/getting_started/create_your_first_expectations.md).
- Configured a [Checkpoint](../../../tutorials/getting_started/validate_your_data.md).
- Installed [boto3](https://github.com/boto/boto3) in your local environment.
- Identified the S3 bucket and prefix where Validation results will be stored.

</Prerequisites>

## Steps

### 1. **Configure** [boto3](https://github.com/boto/boto3) **to connect to the Amazon S3 bucket where Validation results will be stored**

Instructions on how to set up [boto3](https://github.com/boto/boto3) with AWS can be found at boto3's [documentation site](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

### 2. **Identify your Data Context Validations Store**

Look for the following section in your <TechnicalTag relative="../../../" tag="data_context" text="Data Context's" /> ``great_expectations.yml`` file:

```yaml
validations_store_name: validations_store

stores:
  validations_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
```
The configuration file tells Great Expectations to look for Validations in a store called ``validations_store``. It also creates a ``ValidationsStore`` called ``validations_store`` that is backed by a Filesystem and will store validations under the ``base_directory`` ``uncommitted/validations`` (the default).

### 3. **Update your configuration file to include a new store for Validation results on S3**

In the example below, the new store's name is set to ``validations_S3_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleS3StoreBackend``, ``bucket`` will be set to the address of your S3 bucket, and ``prefix`` will be set to the folder in your S3 bucket where Validation results will be located.

:::caution
If you are also storing Expectations in S3 ([How to configure an Expectation store to use Amazon S3](./how_to_configure_an_expectation_store_in_amazon_s3.md)), or DataDocs in S3 ([How to host and share Data Docs on Amazon S3](../configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3.md)), then please ensure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

```yaml
validations_store_name: validations_S3_store

stores:
  validations_S3_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleS3StoreBackend
          bucket: '<your_s3_bucket_name>'
          prefix: '<your_s3_bucket_folder_name>'
```

### 4. **Copy existing Validation results to the S3 bucket** (This step is optional)

One way to copy Validations into Amazon S3 is by using the ``aws s3 sync`` command.  As mentioned earlier, the ``base_directory`` is set to ``uncommitted/validations/`` by default. In the example below, two Validation results, ``Validation1`` and ``Validation2`` are copied to Amazon S3.  Your output should looks something like this:

```bash
aws s3 sync '<base_directory>' s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'
upload: uncommitted/validations/val1/val1.json to s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'/val1.json
upload: uncommitted/validations/val2/val2.json to s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'/val2.json
```

### 5. **Confirm that the new Validations store has been added by running** ``great_expectations store list``

Notice the output contains two Validations Stores: the original ``validations_store`` on the local filesystem and the ``validations_S3_store`` we just configured.  This is ok, since Great Expectations will look for Validation results on the S3 bucket as long as we set the ``validations_store_name`` variable to ``validations_S3_store``.

```bash
great_expectations store list

- name: validations_store
 class_name: ValidationsStore
 store_backend:
   class_name: TupleFilesystemStoreBackend
   base_directory: uncommitted/validations/

- name: validations_S3_store
 class_name: ValidationsStore
 store_backend:
   class_name: TupleS3StoreBackend
   bucket: '<your_s3_bucket_name>'
   prefix: '<your_s3_bucket_folder_name>'
```

### 6. **Confirm that the Validations store has been correctly configured**

Run a [Checkpoint](../../../tutorials/getting_started/validate_your_data.md) to store results in the new Validations store on S3 then visualize the results by re-building [Data Docs](../../../tutorials/getting_started/check_out_data_docs.md).
