---
title: Connect to data
---

Once you have a Data Context, you’ll want to connect to data. In Great Expectations, Datasources simplify connections, by managing configuration and providing a consistent, cross-platform API for referencing data.

Let’s configure your first Datasource: a connection to the data directory we’ve provided in the repo. This could also be a database connection, but for now we’re just using a simple file store.

Start by running the following command:
````console
great_expectations datasource new
````

````console
What data would you like Great Expectations to connect to?
    1. Files on a filesystem (for processing with Pandas or Spark)
    2. Relational database (SQL)
: 1

What are you processing your files with?
    1. Pandas
    2. PySpark
: 1

Enter the path of the root directory where the data files are stored. If files are on local disk
enter a path relative to your current working directory or an absolute path.
: data
````

This will now **open up a new Jupyter Notebook** to complete the Datasource configuration.

### The ```datasource new``` notebook

The Jupyter Notebook contains some boilerplate code to configure your new Datasource. You can run the entire notebook as-is, but we recommend changing at least the Datasource name to something more specific.

Edit the second code cell as follows:

````console
datasource_name = "getting_started_datasource"
````

Then **execute all cells in the notebook** in order to save the new Datasource. If successful, the last cell will print a list of all Datasources, including the one you just created.

**Before continuing, let’s stop and unpack what just happened.**

### Configuring Datasources

When you completed those last few steps, you told Great Expectations that:

+ You want to create a new Datasource called `getting_started_datasource`.
+ You want to use Pandas to read the data from CSV.

Based on that information, the CLI added the following entry into your ```great_expectations.yml``` file, under the datasources header:

```yaml file=../../../tests/integration/docusaurus/tutorials/getting-started/getting_started.py#L23-L40
```

<details>
  <summary>What does the configuration contain?</summary>
  <div>
    <p>
        <b>ExecutionEngine</b> : The <code>ExecutionEngine</code> provides backend-specific computing resources that are used to read-in and perform validation on data.  For more information on `ExecutionEngines`, please refer to the following [Core Concepts document on ExecutionEngines](/docs/reference/execution_engine)
    </p>
    <p>
        <b>DataConnectors</b> :  <code>DataConnectors</code> facilitate access to external data stores, such as filesystems, databases, and cloud storage. The current configuration contains both an <code>InferredAssetFilesystemDataConnector</code>, which allows you to retrieve a batch of data by naming a data asset (which is the filename in our case), and a <code>RuntimeDataConnector</code>, which allows you to retrieve a batch of data by defining a filepath.  In this tutorial we will only be using the <code>InferredAssetFilesystemDataConnector</code>.  For more information on <code>DataConnectors</code>, please refer to the <a href="https://docs.greatexpectations.io/docs/reference/datasources">Core Concepts document on Datasources</a>.
    </p>
    <p>
        This datasource does not require any credentials. However, if you were to connect to a database that requires connection credentials, those would be stored in <code>great_expectations/uncommitted/config_variables.yml</code>.
    </p>
  </div>
</details>

In the future, you can modify or delete your configuration by editing your ```great_expectations.yml``` and ```config_variables.yml``` files directly.

**For now, let’s move on to creating your first Expectations.**
