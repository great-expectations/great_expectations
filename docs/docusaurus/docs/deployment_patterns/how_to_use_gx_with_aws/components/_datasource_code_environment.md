import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

The previous sections of this guide involved manually editing configuration files to add configurations for Amazon S3 buckets.  When setting up your Datasource configurations, it is simpler to use Great Expectation's Python API.  We recommend doing this from a Jupyter Notebook, as you will then receive immediate feedback on the results of your code blocks.  However, you can alternatively use a Python script in the IDE of your choice.

If you would like, you may use the Great Expectations <TechnicalTag tag="cli" text="CLI" /> to automatically generate a pre-configured Jupyter Notebook. To do so, run the following console command from the root directory of your Great Expectations project:

```console
great_expectations datasource new
```

Once you have your pre-configured Jupyter Notebook, you should follow along in the YAML-based workflow in the following steps.

If you choose to work from a blank Jupyter Notebook or a Python script, you may find it easier to use the following Python dictionary workflow over the YAML workflow.  Great Expectations supports either configuration method.