First, create a Batch Request.

```python name="tests/integration/docusaurus/connecting_to_your_data/database/athena_python_example.py Batch Request"

```

Next, prepare an empty Expectation suite.

```python name="tests/integration/docusaurus/connecting_to_your_data/database/athena_python_example.py Create Expectation Suite"

```

Now you can load data into a `Validator`.  If this is successful then you will have verified that your Data Source is working properly.

```python name="tests/integration/docusaurus/connecting_to_your_data/database/athena_python_example.py Test Data Source with Validator"

```