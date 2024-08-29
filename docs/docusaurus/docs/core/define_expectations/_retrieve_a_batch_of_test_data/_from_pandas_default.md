import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPython from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md';

The `pandas_default` Data Source is built into every Data Context and can be found at `.data_sources.pandas_default` on your Data Context.

The `pandas_default` Data Source provides methods to read the contents of a single datafile in any format supported by pandas.  These `.read_*(...)` methods do not create a Data Asset or Batch Definition for the datafile.  Instead, they simply return a Batch of data.

Because the `pandas_default` Data Source's `.read_*(...)` methods only return a Batch and do not save configurations for reading files to the Data Context, they are less versatile than a fully configured Data Source, Data Asset, and Batch Definition. Therefore, the `pandas_default` Data Source is only intended to facilitate testing Expectations and engaging in data exploration.  The `pandas_default` Data Source's `.read_*(...)` methods are less suited for use in production and automated workflows.

## Prerequisites

- <PrereqPython/>.
- <PrereqGxInstallation/>.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.
- Data in a file format supported by pandas, such as `.csv` or `.parquet`.

### Procedure

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Define the path to the datafile.

   The simplest method is to provide an absolute path to the datafile that you will retrieve records from.  However, if you are using a File Data Context you can also provide a path relative to the Data Context's `base_directory`.

   The following example specifies a `.csv` datafile using a relative path:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - provide the path to a data file"
   ```

2. Use the appropriate `.read_*(...)` method of the `pandas_default` Data Source to retrieve a Batch of data.

   The `pandas_default` Data Source can read any file format supported by your current installation of pandas.

   The `.read_*(...)` methods of the `pandas_default` Data Source will return a Batch that contains all of the records in the provided datafile.

   The following example reads a `.csv` file into a Batch of data:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - read data into Batch with pandas_default Data Source"
   ```

   GX supports all of the pandas `.read_*(...)` methods.  For more information on which Pandas `read_*` methods are available, please reference [the official Pandas Input/Output documentation](https://pandas.pydata.org/docs/reference/io.html) for the version of Pandas that you have installed.

3. Optional. Verify that the returned Batch is populated with records.

   You can verify that your Batch Definition was able to read in data and return a populated Batch by printing the header and first few records of the returned Batch:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - verify data was read into a Batch"
   ```


</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - full code example"
```

</TabItem>

</Tabs>