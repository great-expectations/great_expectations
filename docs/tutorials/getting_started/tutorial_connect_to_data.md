---
title: 'Tutorial, Step 2: Connect to data'
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '/docs/term_tags/_tag.mdx';

<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/> 

:::note Prerequisites

- Completed [Step 1: Setup](./tutorial_setup.md) of this tutorial.

:::

In Step 1: Setup, we created a <TechnicalTag relative="../../" tag="data_context" text="Data Context" />.  Now that we have that Data Context, you'll want to connect to your actual data.  In Great Expectations, <TechnicalTag relative="../../" tag="datasource" text="Datasources" /> simplify these connections by managing and providing a consistent, cross-platform API for referencing data.

### Create a Datasource with the CLI
Let's create and configure your first Datasource: a connection to the data directory we've provided in the repo.  This could also be a database connection, but because our tutorial data consists of .CSV files we're just using a simple file store.

Start by using the <TechnicalTag relative="../../" tag="cli" text="CLI" /> to run the following command from your `ge_tutorials` directory:

````console
great_expectations datasource new
````

You will then be presented with a choice that looks like this:

````console
What data would you like Great Expectations to connect to?
    1. Files on a filesystem (for processing with Pandas or Spark)
    2. Relational database (SQL)
:1
````

The only difference is that we've included a "1" after the colon and you haven't typed anything in answer to the prompt, yet.

As we've noted before, we're working with .CSV files.  So you'll want to answer with `1` and hit enter.

The next prompt you see will look like this:
````console
What are you processing your files with?
    1. Pandas
    2. PySpark
:1
````

For this tutorial we will use Pandas to process our files, so again answer with `1` and press enter to continue.

We're almost done with the CLI!  You'll be prompted once more, this time for the path of the directory where the data files are located.  The prompt will look like:

````console
Enter the path of the root directory where the data files are stored. If files are on local disk
enter a path relative to your current working directory or an absolute path.
:data
````

The data that this tutorial uses is stored in `ge_tutorials/data`.  Since we are working from the `ge_tutorials` directory, you only need to enter `data` and hit return to continue.

This will now **open up a new Jupyter Notebook** to complete the Datasource configuration.  Your console will display a series of messages as the Jupyter Notebook is loaded, but you can disregard them.  The rest of the Datasource setup takes place in the Jupyter Notebook and we won't return to the terminal until that is done.

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

Based on that information, the CLI added the following entry into your ```great_expectations.yml``` file, under the `datasources` header:

```yaml file=../../../tests/integration/docusaurus/tutorials/getting-started/getting_started.py#L23-L40
```

Please note that due to how data is serialized, the entry in your ```great_expectations.yml``` file may not have these key/value pairs in the same order as the above example.  However, they will all have been added.

<details>
  <summary>What does the configuration contain?</summary>
  <div>
    <p>

**ExecutionEngine** : The <TechnicalTag relative="../../" tag="execution_engine" text="Execution Engine" /> provides backend-specific computing resources that are used to read-in and perform validation on data.  For more information on <code>ExecutionEngines</code>, please refer to the following <a href="/docs/reference/execution_engine">Core Concepts document on ExecutionEngines</a>

</p>
    <p>

**DataConnectors** :  <TechnicalTag relative="../../" tag="data_connector" text="Data Connectors" /> facilitate access to external data stores, such as filesystems, databases, and cloud storage. The current configuration contains both an <code>InferredAssetFilesystemDataConnector</code>, which allows you to retrieve a batch of data by naming a data asset (which is the filename in our case), and a <code>RuntimeDataConnector</code>, which allows you to retrieve a batch of data by defining a filepath.  In this tutorial we will only be using the <code>InferredAssetFilesystemDataConnector</code>.  For more information on <code>DataConnectors</code>, please refer to the <a href="/docs/reference/datasources">Core Concepts document on Datasources</a>.

</p>
    <p>
        This Datasource does not require any credentials. However, if you were to connect to a database that requires connection credentials, those would be stored in <code>great_expectations/uncommitted/config_variables.yml</code>.
    </p>
  </div>
</details>

In the future, you can modify or delete your configuration by editing your ```great_expectations.yml``` and ```config_variables.yml``` files directly.

For now, let’s move on to [Step 3: Create Expectations.](./tutorial_create_expectations.md)

