---
title: How to use a Custom Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Custom <TechnicalTag tag="expectation" text="Expectations"/> are extensions to the core functionality of Great Expectations. Many Custom Expectations may be fit for a very specific purpose, 
or be at lower levels of stability and feature maturity than the core library. 

As such, they are not available for use from the core library, and require registration and import to become available.

This guide will walk you through the process of utilizing Custom Expectations, whether they were built by you or came from the Great Expectations Experimental Library.

<Prerequisites>

- Created a <TechnicalTag tag="custom_expectation" text="Custom Expectation"/> ***or*** identified a Custom Expectation for use from the [Great Expectations Experimental Library](https://github.com/great-expectations/great_expectations/tree/develop/contrib/experimental/great_expectations_experimental/expectations)

</Prerequisites>

## Steps

<Tabs
  groupId="expectation-type"
  defaultValue='custom-expectations'
  values={[
  {label: 'Custom Expectations You\'ve Built', value:'custom-expectations'},
  {label: 'Custom Expectations Contributed To Great Expectations', value:'contrib-expectations'},
  ]}>

<TabItem value="custom-expectations">

### 1. File placement & import

If you're using a Custom Expectation you've built, 
you'll need to place it in the `great_expectations/plugins/expectations` folder of your Great Expectations deployment. 

When you instantiate your <TechnicalTag tag="data_context" text="Data Context"/>, it will automatically make all plugins in the directory available for use, 
allowing you to import your Custom Expectation from that directory whenever and wherever it will be used. 
This import will be needed when an <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> is created, *and* when a <TechnicalTag tag="checkpoint" text="Checkpoint"/> is defined and run.

### 2. Use in a Suite

To use your Custom Expectation, we need to import it.

To do this, we first need to instantiate our Data Context.

For example, a pattern for importing a Custom Expectation `ExpectColumnValuesToBeAlphabetical` could look like:

```python
context = ge.get_context()
from expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical
```

Now that your Custom Expectation has been imported, it is available with the same patterns as the core Expectations:

```python
validator.expect_column_values_to_be_alphabetical(column="test")
```

### 3. Use in a Checkpoint

Once you have your Custom Expectation in a Suite, you will also need to make it available to your Checkpoint.

To do this, we'll need to put together our own Checkpoint script. From your command line, you can execute:

```commandline
great_expectations checkpoint new <my_checkpoint_name>
```

This will open a Jupyter Notebook allowing you to create a Checkpoint. 
If you would like to run your Checkpoint from this notebook, you will need to import your Custom Expectation again as above.

To continue to use this Checkpoint containing a Custom Expectation outside this notebook, we will need to set up a script for your Checkpoint.

To do this, execute the following from your command line:

```commandline
great_expectations checkpoint script <my_checkpoint_name>
```

This will create a script in your GE directory at `great_expectations/uncommitted/run_my_checkpoint_name.py`. 
That script can be edited that script to include the Custom Expectation import(s) you need:

```python
import sys

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext

data_context: DataContext = DataContext(
    context_root_dir="/your/path/to/great_expectations"
)

from expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical

result: CheckpointResult = data_context.run_checkpoint(
    checkpoint_name="my_checkpoint_name",
    batch_request=None,
    run_name=None,
)

if not result["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")
sys.exit(0)
```

The Checkpoint can then be run with:

```python
python great_expectations/uncommitted/run_my_checkpoint_name.py
```

</TabItem>

<TabItem value="contrib-expectations">

### 1. Installation & import

If you're using a Custom Expectation that is coming from the `Great Expectations Experimental` library, 
it will need to either be imported from there directly. To do this, we'll first need to `pip install great_expectations_experimental`. 

Once that is done, you will be able to import directly from that package:

```python
from great_expectations_experimental.expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical
```

This import will be needed when an <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> is created, *and* when a <TechnicalTag tag="checkpoint" text="Checkpoint"/> is defined and run.

### 2. Use in a Suite

To use your Custom Expectation, we need to import it as above.

Once that is done, your Custom Expectation will be available with the same patterns as the core Expectations:

```python
validator.expect_column_values_to_be_alphabetical(column="test")
```

### 3. Use in a Checkpoint

Once you have your Custom Expectation in a Suite, you will also need to make it available to your Checkpoint.

To do this, we'll need to put together our own Checkpoint script. From your command line, you can execute:

```commandline
great_expectations checkpoint new <my_checkpoint_name>
```

This will open a Jupyter Notebook allowing you to create a Checkpoint. 
If you would like to run your Checkpoint from this notebook, you will need to import your Custom Expectation again as above.

To continue to use this Checkpoint containing a Custom Expectation outside this notebook, we will need to set up a script for your Checkpoint.

To do this, execute the following from your command line:

```commandline
great_expectations checkpoint script <my_checkpoint_name>
```

This will create a script in your GE directory at `great_expectations/uncommitted/run_my_checkpoint_name.py`. 
That script can be edited that script to include the Custom Expectation import(s) you need:

```python
import sys

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext
from great_expectations_experimental.expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical


data_context: DataContext = DataContext(
    context_root_dir="/your/path/to/great_expectations"
)

result: CheckpointResult = data_context.run_checkpoint(
    checkpoint_name="my_checkpoint_name",
    batch_request=None,
    run_name=None,
)

if not result["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")
sys.exit(0)
```

The Checkpoint can then be run with:

```python
python great_expectations/uncommitted/run_my_checkpoint_name.py
```

</TabItem>

</Tabs>

<div style={{"text-align":"center"}}>  
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>  
Congratulations!<br/>&#127881; You've just run a Checkpoint with a Custom Expectation! &#127881;  
</b></p>  
</div>
