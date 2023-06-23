---
title: Integrate ZenML with Great Expectations
authors:
    name: Stefan Nica
    url: https://zenml.io
description: "Integrate ZenML with Great Expectations"
sidebar_label: "ZenML"
sidebar_custom_props: { icon: 'img/integrations/zenml_icon.png' }
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

:::info
* Maintained By: ZenML
* Status: Beta
* Support/Contact: https://zenml.io/slack-invite/
:::

[ZenML](https://zenml.io/) helps data scientists and ML engineers to make
Great Expectations data profiling and validation an integral part of their
production ML toolset and workflows. ZenML is [an extensible open source MLOps framework](https://github.com/zenml-io/zenml)
for creating portable, production-ready ML pipelines.

ZenML eliminates the complexity associated with setting up the Great
Expectations <TechnicalTag tag="data_context" text="Data Context" />
for use in production by integrating it directly into its [MLOps tool stack construct](https://docs.zenml.io/getting-started/core-concepts#stacks-components-and-stores).
This allows you to start using Great Expectations in your ML pipelines right
away, with all the other great features that ZenML brings along: portability,
caching, tracking and versioning and immediate access to a rich ecosystem of
tools and services that spans everything else MLOps.

### Prerequisites

 - An understanding of Great Expectations <TechnicalTag tag="expectation_suite" text="Expectation Suites" />, 
 <TechnicalTag tag="validation_result" text="Validation Results"/>, 
 and <TechnicalTag tag="data_docs" text="Data Docs" /> concepts
 - Some understanding of [ZenML pipelines and steps](https://docs.zenml.io/developer-guide/steps-and-pipelines#pipeline).


ZenML ships with a couple of builtin pipeline steps that take care of everything
from configuring temporary <TechnicalTag tag="datasource" text="Datasources" />,
Data Connectors, and <TechnicalTag tag="batch_request" text="Runtime Batch Requests" /> 
to access in-memory datasets to setting up and running <TechnicalTag tag="profiler" text="Profilers" />, <TechnicalTag tag="validator" text="Validators" /> and <TechnicalTag tag="checkpoint" text="Checkpoints" />, to generating the <TechnicalTag tag="data_docs" text="Data Docs" /> 
for you. These details are abstracted away from you and all you have left
to do is simply insert these steps into your ML pipelines to run either data
profiling or data validation with Great Expectations on any input Pandas
DataFrame.

Also included is a ZenML visualizer that gives you quick access to the Data Docs
to display Expectation Suites and Validation Results generated, versioned and
stored by your pipeline runs.

### Dev loops unlocked by integration
* Implement [the Great Expectations "golden path" workflow](https://greatexpectations.io/blog/ml-ops-great-expectations):
teams can create Expectation Suites and store them in the shared ZenML Artifact
Store, then use them in their ZenML pipelines to automate data validation. All
this with Data Docs providing a complete report of the overall data quality
status of your project.
* Start with Great Expectations hosted exclusively on your local machine and
then incrementally migrate to production ready ZenML MLOps stacks as your
project matures. With no code changes or vendor lock-in.

### Setup
This simple setup installs both Great Expectations and ZenML and brings them
together into a single MLOps local stack. More elaborate configuration options
are of course possible and thoroughly documented in [the ZenML documentation](https://docs.zenml.io/mlops-stacks/data-validators/great-expectations).

#### 1. Install ZenML
```shell
pip install zenml
```

#### 2. Install the Great Expectations ZenML integration
```shell
zenml integration install -y great_expectations
```

#### 3. Add Great Expectations as a Data Validator to the default ZenML stack
```shell
zenml data-validator register ge_data_validator --flavor great_expectations
zenml stack update -dv ge_data_validator
```

:::tip
This stack uses the default local [ZenML Artifact Store](https://docs.zenml.io/mlops-stacks/artifact-stores)
that persists the Great Expectations Data Context information on your local
machine. However, you can use any of [the Artifact Store flavors](https://docs.zenml.io/mlops-stacks/artifact-stores#artifact-store-flavors)
shipped with ZenML, like AWS, GCS or Azure. They will all work seamlessly with Great
Expectations.
:::


## Usage
Developing ZenML pipelines that harness the power of Great Expectations to
perform data profiling and validation is just a matter of instantiating the
builtin ZenML steps and linking them to other steps that ingest data. The
following examples showcase two simple pipeline scenarios that do exactly that.

:::info
To run the examples, you will also need to install the ZenML scikit-learn
integration:

```shell
zenml integration install -y sklearn
```
:::

### Great Expectations Zenml data profiling example
This is a simple example of a ZenML pipeline that loads data from a source and
then uses the ZenML builtin Great Expectations profiling step to infer an
Expectation Suite from that data. After the pipeline run is complete, the
Expectation Suite can be visualized in the Data Docs.

:::tip
The following Python code is fully functional. You can simply copy it in a file
and run it as-is, assuming you installed and setup ZenML properly.
:::


```python
import pandas as pd
from sklearn import datasets

from zenml.integrations.constants import GREAT_EXPECTATIONS, SKLEARN
from zenml.integrations.great_expectations.steps import (
    GreatExpectationsProfilerConfig,
    great_expectations_profiler_step,
)
from zenml.integrations.great_expectations.visualizers import (
    GreatExpectationsVisualizer,
)
from zenml.pipelines import pipeline
from zenml.steps import Output, step


#### 1. Define ZenML steps


@step(enable_cache=False)
def importer(
) -> Output(dataset=pd.DataFrame, condition=bool):
    """Load and return a random sample of the University of Wisconsin breast
    cancer diagnosis dataset.
    """
    breast_cancer = datasets.load_breast_cancer()
    df = pd.DataFrame(
        data=breast_cancer.data, columns=breast_cancer.feature_names
    )
    df["class"] = breast_cancer.target
    return df.sample(frac = 0.5), True

# instantiate a builtin Great Expectations data profiling step
ge_profiler_step = great_expectations_profiler_step(
    step_name="ge_profiler_step",
    config=GreatExpectationsProfilerConfig(
        expectation_suite_name="breast_cancer_suite",
        data_asset_name="breast_cancer_df",
    )
)


#### 2. Define the ZenML pipeline


@pipeline(required_integrations=[SKLEARN, GREAT_EXPECTATIONS])
def profiling_pipeline(
    importer, profiler
):
    """Data profiling pipeline for Great Expectations."""
    dataset, _ = importer()
    profiler(dataset)


#### 4. Instantiate and run the pipeline


profiling_pipeline(
    importer=importer(),
    profiler=ge_profiler_step,
).run()


#### 5. Visualize the Expectation Suite generated, tracked and stored by the pipeline


last_run = profiling_pipeline.get_runs()[-1]
step = last_run.get_step(name="profiler")
GreatExpectationsVisualizer().visualize(step)
```

### Great Expectations Zenml data validation example
This is a simple example of a ZenML pipeline that loads data from a source and
then uses the ZenML builtin Great Expectations data validation step to validate
that data against an existing Expectation Suite and generate Validation Results.
After the pipeline run is complete, the Validation Results can be visualized in
the Data Docs.

:::info
This example assumes that you already have an Expectations Suite named
`breast_cancer_suite` that has been previously stored in the Great Expectations
Data Context. You should run [the Great Expectations Zenml data profiling example](#great-expectations-zenml-data-profiling-example)
first to ensure that, or create one by other means.
:::

:::tip
The following Python code is fully functional. You can simply copy it in a file
and run it as-is, assuming you installed and setup ZenML properly.
:::


```python
import pandas as pd
from great_expectations.checkpoint.types.checkpoint_result import (
    CheckpointResult,
)
from sklearn import datasets

from zenml.integrations.constants import GREAT_EXPECTATIONS, SKLEARN
from zenml.integrations.great_expectations.steps import (
    GreatExpectationsValidatorConfig,
    great_expectations_validator_step,
)
from zenml.integrations.great_expectations.visualizers import (
    GreatExpectationsVisualizer,
)
from zenml.pipelines import pipeline
from zenml.steps import Output, step


#### 1. Define ZenML steps


@step(enable_cache=False)
def importer(
) -> Output(dataset=pd.DataFrame, condition=bool):
    """Load and return a random sample of the University of Wisconsin breast
    cancer diagnosis dataset.
    """
    breast_cancer = datasets.load_breast_cancer()
    df = pd.DataFrame(
        data=breast_cancer.data, columns=breast_cancer.feature_names
    )
    df["class"] = breast_cancer.target
    return df.sample(frac = 0.5), True

# instantiate a builtin Great Expectations data profiling step
ge_validator_step = great_expectations_validator_step(
    step_name="ge_validator_step",
    config=GreatExpectationsValidatorConfig(
        expectation_suite_name="breast_cancer_suite",
        data_asset_name="breast_cancer_test_df",
    )
)

@step
def analyze_result(
    result: CheckpointResult,
) -> bool:
    """Analyze the Great Expectations validation result and print a message
    indicating whether it passed or failed."""
    if result.success:
        print("Great Expectations data validation was successful!")
    else:
        print("Great Expectations data validation failed!")
    return result.success


#### 2. Define the ZenML pipeline


@pipeline(required_integrations=[SKLEARN, GREAT_EXPECTATIONS])
def validation_pipeline(
    importer, validator, checker
):
    """Data validation pipeline for Great Expectations."""
    dataset, condition = importer()
    results = validator(dataset, condition)
    checker(results)


#### 4. Instantiate and run the pipeline


validation_pipeline(
    importer=importer(),
    validator=ge_validator_step,
    checker=analyze_result(),
).run()


#### 5. Visualize the Validation Results generated, tracked and stored by the pipeline


last_run = validation_pipeline.get_runs()[-1]
step = last_run.get_step(name="validator")
GreatExpectationsVisualizer().visualize(step)
```


## Further discussion

### Things to consider
The Great Expectations builtin ZenML steps and visualizer are a quick and
convenient way of bridging the data validation and ML pipelines domains, but
this convenience comes at a cost: there is little flexibility in the way of
dataset types and configurations for Great Expectations Checkpoints, Profiles
and Validators.

If the builtin ZenML steps are insufficient, you can always implement your own
custom ZenML pipeline steps that use Great Expectations while still benefiting
from the other ZenML integration features:

* the convenience of using a Great Expectations Data Context that is
automatically configured to connect to the infrastructure of your choise
* the ability to version, track and visualize Expectation Suites and Validation
Results as pipeline artifacts
* the freedom that comes from being able to combine Great Expectations with
a wide range of libraries and services in the ZenML MLOps ecosystem providing
functions like ML pipeline orchestration, experiment and metadata tracking,
model deployment, data annotation and a lot more


### When things don't work

- Refer to [the ZenML documentation](https://docs.zenml.io/mlops-stacks/data-validators/great-expectations)
for in-depth instructions on how to configure and use Great Expectations with
ZenML.
- Reach out to the ZenML community [on Slack](https://zenml.io/slack-invite/)
and ask for help.

### Other resources

 - This [ZenML blog post](https://blog.zenml.io/great-expectations/) covers
 the Great Expectations integration and includes a full tutorial. 
 - [A similar example](https://github.com/zenml-io/zenml/tree/main/examples/great_expectations_data_validation)
 is included in the ZenML list of code examples. [A Jupyter notebook](https://colab.research.google.com/github/zenml-io/zenml/blob/main/examples/great_expectations_data_validation/great_expectations.ipynb)
 is included.
 - A recording of [the Great Expectation integration demo](https://www.youtube.com/watch?v=JIoTrHL1Dmk)
 done in one of the ZenML community hour meetings.
 - Consult [the ZenML documentation](https://docs.zenml.io/mlops-stacks/data-validators/great-expectations)
for more information on how to use Great Expectations together with ZenML.
