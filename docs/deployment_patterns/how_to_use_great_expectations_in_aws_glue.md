---
title: How to Use Great Expectations in AWS Glue
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'
import Congratulations from '../guides/connecting_to_your_data/components/congratulations.md'

This Guide demonstrates how to set up, initialize and run validations against your data on AWS Glue Spark Job.
We will cover case with RuntimeDataConnector and use S3 as metadata store.

### 0. Pre-requirements

- Configure great_expectations.yaml and upload to your S3 bucket or generate it dynamically from code
```yaml file=../../tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns_great_expectations.yaml#L1-L67
```


### 1. Install Great Expectations
You need to add to your AWS Glue Spark Job Parameters to install great expectations module. Glue at least v2
```bash
  — additional-python-modules great_expectations
```
Then import necessary libs:
```python file=../../tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns.py#L1-L9
```

### 2. Set up Great Expectations
Here we initialize a Spark and Glue, and read great_expectations.yaml
```python file=../../tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns.py#L11-L16
```

### 3. Connect to your data
```python file=../../tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns.py#L18-L29
```

### 4. Create Expectations
```python file=../../tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns.py#L31-L42
```

### 5. Validate your data
```python file=../../tests/integration/docusaurus/deployment_patterns/aws_glue_deployment_patterns.py#L44-L50
```

### 6. Congratulations!
Your data docs built on S3 and you can see index.html at the bucket
