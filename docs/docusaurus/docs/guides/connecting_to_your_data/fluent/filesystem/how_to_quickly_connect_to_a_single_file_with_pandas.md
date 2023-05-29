---
title: How to quickly connect to a single file using Pandas
tag: [how-to, connect to data]
description: A technical guide on using Pandas to immediately connect GX with the data in a single source file.
keywords: [Great Expectations, Pandas, Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- Introduction -->
import Introduction from '/docs/components/connect_to_data/filesystem/_intro_connect_to_one_or_more_files_pandas_or_spark.mdx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

import InfoUsingPandasToConnectToDifferentFileTypes from '/docs/components/connect_to_data/filesystem/_info_using_pandas_to_connect_to_different_file_types.mdx'

<!-- Next steps -->
import AfterCreateValidator from '/docs/components/connect_to_data/next_steps/_after_create_validator.md'

<Introduction execution_engine="Pandas" />

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to source data stored in a filesystem

</Prerequisites> 

## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Specify a file to read into a Data Asset

Great Expectations supports reading the data in individual files directly into a Validator using Pandas.  To do this, we will run the code:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_quickly_connect_to_a_single_file_with_pandas.py get_validator"
```

<InfoUsingPandasToConnectToDifferentFileTypes this_example_file_extension="csv"/>

## Next steps

Now that you have a Validator, you can immediately move on to creating Expectations.  For more information, please see:

<AfterCreateValidator />
