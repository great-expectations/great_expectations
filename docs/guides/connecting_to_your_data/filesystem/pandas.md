---
title: How to connect to your data on a filesystem using pandas
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'

This guide will help you connect to your data stored on a filesystem using pandas. This enables you to work with your data in Great Expectations.

:::note Prerequisites: This how-to guide assumes you have already:
- Completed the [Getting Started Tutorial](../../../tutorials/getting-started/intro.md)
- Have a working installation of Great Expectations
- Have access to data on a filesystem
:::

## Steps



<Tabs
  defaultValue="python"
  values={[
    {label: 'CLI', value: 'cli'},
    {label: 'python', value: 'python'},
  ]}>
  <TabItem value="cli">

### 1. Run the following CLI command

```console
great_expectations --v3-api datasource new
```


### 2. Modify your YAML configuration if necessary 

The CLI will go through some steps where you will add the configuration. The notebook will open and will contain the configuration you see here: 

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_cli.py#L9-L26
```

Make sure the information is correct, and make any changes if necessary

### 3. Test the YAML configuration 

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_cli.py#L28
```

### 4. Save Datasource to Data context

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_cli.py#L30
```

### 5. Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.
 

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_cli.py#L35-L46
```

  </TabItem>
  <TabItem value="python">


### 1. Load your project's DataContext into memory

Create a Jupyter notebook or script in the same directory as the `great_expectations/` directory.
Import these necessary packages and modules.

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L1-L4
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L6
```


### 2. Write your YAML Datasource configuration

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L8-L20
```


### 3. Save your Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L22
```


### 3. Test your new Datasource
Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

Here is an example BatchRequest which will. Replace it with the path to the file you would liek to read in.  

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L24-L30
```

Get a Validator and print out the top few row. 

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L38-L44
```

</TabItem>
</Tabs>


<Congratulations />

## Next Steps

<NextSteps />

## Additional Notes

To view the full script [see it on GitHub](https://github.com/great-expectations/great_expectations/blob/knoxpod/integration/code/connecting_to_your_data/database/postgres.py)
