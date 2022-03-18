---
title: How to use a Custom Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'

Custom Expectations are extensions to the core functionality of Great Expectations. Many Custom Expectations may be fit for a very specific purpose, 
or be at lower levels of stability and feature maturity than the core library. 

As such, they are not available for use from the core library, and require registration and import to become available.

This guide will walk you through the process of utilizing Custom Expectations.

<Prerequisites>

- Created a [Custom Expectation](./overview.md) ***or*** identified a Custom Expectation for use from the [Great Expectations Experimental Library](https://github.com/great-expectations/great_expectations/tree/develop/contrib/experimental/great_expectations_experimental/expectations)

</Prerequisites>

## Custom Expectations You've Built

### 1. File placement & import

If you're using a Custom Expectation you've built (i.e., one that isn't coming from `contrib/experimental/great_expectations_experimental`), 
you'll need to put it in the `great_expectations/plugins/expectations` folder of your Great Expectations deployment. 

When you instantiate the corresponding DataContext, it will automatically make all plugins in the directory available for use, 
allowing you to import your Custom Expectation from that directory whenever and wherever it will be used. 
This will import will be needed when an Expectation Suite is created, *and* when the checkpoint is defined and run.

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

<div style={{"text-align":"center"}}>  
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>  
Congratulations!<br/>&#127881; You've just run a Checkpoint with a Custom Expectation! &#127881;  
</b></p>  
</div>

If you're using a Custom Expectation that is coming from contrib/experimental/great_expectations_experimental, 
it will need to either be imported from there directly, or by installing the package with pip install great_expectations_experimental and importing from the installed package.

For example, a pattern for importing ExpectColumnValuesToBeAlphabetical could look like:
from great_expectations_experimental.expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical


In your situation, we'll need to put together our own checkpoint script with great_expectations checkpoint script mycheckpoint.
This will create a script in your GE directory at  great_expectations/uncommitted/run_mycheckpoint.py . You can edit that script to include the Custom Expectation import(s) you need. The checkpoint can then be run with python great_expectations/uncommitted/run_mycheckpoint.py. 