
[![PyPI](https://img.shields.io/pypi/v/great_expectations)](https://pypi.org/project/great-expectations/#history)
[![PyPI Downloads](https://img.shields.io/pypi/dm/great-expectations)](https://pypistats.org/packages/great-expectations)
[![Build Status](https://img.shields.io/azure-devops/build/great-expectations/bedaf2c2-4c4a-4b37-87b0-3877190e71f5/1)](https://dev.azure.com/great-expectations/great_expectations/_build/latest?definitionId=1&branchName=develop)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5683574.svg)](https://doi.org/10.5281/zenodo.5683574)
[![Twitter Follow](https://img.shields.io/twitter/follow/expectgreatdata?style=social)](https://twitter.com/expectgreatdata)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://greatexpectations.io/slack)
[![Contributors](https://img.shields.io/github/contributors/great-expectations/great_expectations)](https://github.com/great-expectations/great_expectations/graphs/contributors)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/charliermarsh/ruff)

<!-- <<<Super-quickstart links go here>>> -->



<img align="right" src="./docs/docusaurus/static/img/gx-mark-160.png">

Great Expectations
================================================================================

*Always know what to expect from your data.*

What is GX?
--------------------------------------------------------------------------------

Great Expectations (GX) helps data teams build a shared understanding of their data through quality testing, documentation, and profiling.

Data practitioners know that testing and documentation are essential for managing complex data pipelines. GX makes it possible for data science and engineering teams to quickly deploy extensible, flexible data quality testing into their data stacks. Its human-readable documentation makes the results accessible to technical and nontechnical users.

[See Down with Pipeline Debt!](https://greatexpectations.io/blog/down-with-pipeline-debt-introducing-great-expectations/) for an introduction to our philosophy of pipeline data quality testing.



<!--
--------------------------------------------------
<<<A bunch of logos go here for social proof>>>

--------------------------------------------------
-->

Key features
--------------------------------------------------

### Seamless operation

GX fits into your existing tech stack, and can integrate with your CI/CD pipelines to add data quality exactly where you need it. Connect to and validate your data wherever it already is, so you can focus on honing your Expectation Suites to perfectly meet your data quality needs.

### Start fast

Get useful results quickly even for large data volumes. GX’s Data Assistants provide curated Expectations for different domains, so you can accelerate your data discovery to rapidly deploy data quality throughout your pipelines. Auto-generated Data Docs ensure your DQ documentation will always be up-to-date.



![data_assistant_plot_expectations_and_metrics](./docs/readme_assets/data_assistant_plot_expectations_and_metrics.png)

### Unified understanding

Expectations are GX’s workhorse abstraction: each Expectation declares an expected state of the data. The Expectation library provides a flexible, extensible vocabulary for data quality—one that’s human-readable, meaningful for technical and nontechnical users alike. Bundled into Expectation Suites, Expectations are the ideal tool for characterizing exactly what you expect from your data.

- `expect_column_values_to_not_be_null`
- `expect_column_values_to_match_regex`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_match_strftime_format`
- `expect_table_row_count_to_be_between`
- `expect_column_median_to_be_between`
- ...and [many more](https://greatexpectations.io/expectations)

### Secure and transparent

GX doesn’t ask you to exchange security for your insight. It processes your data in place, on your systems, so your security and governance procedures can maintain control at all times. And because GX’s core is and always will be open source, its complete transparency is the opposite of a black box.

### Data contracts support

Checkpoints are a transparent, central, and automatable mechanism for testing Expectations and evaluating your data quality. Every Checkpoint run produces human-readable Data Docs reporting the results. You can also configure Checkpoints to take Actions based on the results of the evaluation, like sending alerts and preventing low-quality data from moving further in your pipelines.

![Image of data contact support](./docs/readme_assets/data-contract-support.png)

### Readable for collaboration

Everyone stays on the same page about your data quality with GX’s inspectable, shareable, and human-readable Data Docs. You can publish Data Docs to the locations where you need them in a variety of formats, making it easy to integrate Data Docs into your existing data catalogs, dashboards, and other reporting and data governance tools.

![Image of data docs](./docs/readme_assets/datadocs-update.jpg)

Quick start
-------------------------------------------------------------

To see Great Expectations in action on your own data:

You can install it using pip
```
pip install great_expectations
```
and then run

```
import great_expectations as gx

data_context = gx.get_context()
```

(We recommend deploying within a virtual environment. If you’re not familiar with pip, virtual environments, notebooks, or git, you may want to check out the [Supporting Resources](https://docs.greatexpectations.io/docs/terms/supporting_resource/), which will teach you how to get up and running in minutes.)

For full documentation, visit [https://docs.greatexpectations.io/](https://docs.greatexpectations.io/).

If you need help, hop into our [Slack channel](https://greatexpectations.io/slack)&mdash;there are always contributors and other users there.


Integrations
-------------------------------------------------------------------------------
Great Expectations works with the tools and systems that you're already using with your data, including:

<table style="background-color: #fff;">
	<thead>
		<tr>
			<th colspan="2">Integration</th>
			<th>Notes</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/datahub_logo.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/integrations/integration_datahub/">
					DataHub
				</a>
			</td>
			<td>
				Data Catalog
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/aws-glue-logo__1_.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_aws_glue/">
					AWS Glue
				</a>
			</td>
			<td>
				Data Integration
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/Athena_logo.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/athena/">
					Athena
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/datasource_redshift.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/redshift/">
					AWS Redshift
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/awss3.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_amazon_s3/">
					AWS S3
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/bigquery.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_with_google_cloud_platform_and_bigquery/">
					BigQuery
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/og-databricks.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_databricks/">
					Databricks
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/deepnote_logo_400x.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_deepnote/">
					Deepnote
				</a>
			</td>
			<td>
				Collaborative data notebook
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/Google_Cloud_logo.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_with_google_cloud_platform_and_bigquery/">
					Google Cloud Platform &#40;GCP&#41;
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/microsoft-azure-blob-storage-logo.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_azure_blob_storage/">
					Microsoft Azure Blob Storage
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/microsoft-sql-server-logo-96AF49E2B3-seeklogo.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/mssql">
					Microsoft SQL Server
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/mysql.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/mysql/">
					MySQL
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/pandas.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/in_memory/pandas/">
					Pandas
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/postgres.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/postgres/">
					PostgreSQL
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/snowflake.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/snowflake/">
					Snowflake
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/spark.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster/">
					Spark
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/SQLite370.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/sqlite/">
					SQLite
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/trino-og.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/trino/">
					Trino
				</a>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/Apache_Airflow.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_with_airflow/">
					Apache Airflow
				</a>
			</td>
			<td>
				Orchestrator
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/flyte_logo.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_in_flyte/">
					Flyte
				</a>
			</td>
			<td>
				Orchestrator
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/meltano-logo.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_with_meltano/">
					Meltano
				</a>
			</td>
			<td>
				Orchestrator
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/prefect-logo-gradient-navy.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_with_prefect/">
					Prefect
				</a>
			</td>
			<td>
				Orchestrator
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/zenml.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/integrations/integration_zenml/">
					ZenML
				</a>
			</td>
			<td>
				Orchestrator
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/slack.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action/">
					Slack
				</a>
			</td>
			<td>
				Plugin
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/readme_assets/jupyter.jpg" />
			</td>
			<td style="width: 200px;">
				<a href="https://docs.greatexpectations.io/docs/tutorials/getting_started/tutorial_create_expectations/#creating-expectations-in-jupyter-notebooks">
					Jupyter Notebooks
				</a>
			</td>
			<td>
				Utility
			</td>
		</tr>
	</tbody>
</table>


What is GX _not_?
-------------------------------------------------------------

Great Expectations is _not_ a pipeline execution framework. Instead, it integrates seamlessly with DAG execution tools like [Spark]( https://spark.apache.org/), [Airflow](https://airflow.apache.org/), [dbt]( https://www.getdbt.com/), [prefect](https://www.prefect.io/), [dagster]( https://github.com/dagster-io/dagster), [Kedro](https://github.com/quantumblacklabs/kedro), [Flyte](https://flyte.org/), etc. GX carries out your data quality pipeline testing while these tools execute the pipelines.

Great Expectations is _not_ a database or storage software. It processes your data in place, on your existing systems. Expectations and Validation Results that GX produces are metadata about your data.

Great Expectations is _not_ a data versioning tool. If you want to bring your data itself under version control, check out tools like [DVC](https://dvc.org/), [Quilt](https://github.com/quiltdata/quilt), and [lakeFS](https://github.com/treeverse/lakeFS/).

Great Expectations is _not_ a language-agnostic platform. Instead, it follows the philosophy of “take the compute to the data” by using the popular Python language to support native execution of Expectations in pandas, SQL (via SQLAlchemy), and Spark environments.

Great Expectations is _not_ exclusive to Python programming environments. It can be invoked from the command line without a Python environment. However, if you’re working into another ecosystem, you may want to explore ecosystem-specific alternatives such as [assertR](https://github.com/ropensci/assertr) (for R environments) or [TFDV](https://www.tensorflow.org/tfx/guide/tfdv) (for Tensorflow environments).


Who maintains Great Expectations?
-------------------------------------------------------------

Great Expectations OSS is under active development by GX Labs and the Great Expectations community.

What's the best way to get in touch with the Great Expectations team?
--------------------------------------------------------------------------------

If you have questions, comments, or just want to have a good old-fashioned chat about data quality, please hop on our public [Slack channel](https://greatexpectations.io/slack) channel or post in our [GitHub Discussions](https://github.com/great-expectations/great_expectations/discussions).


Can I contribute to the library?
--------------------------------------------------------------------------------

Absolutely. Yes, please. See [Contributing code](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md), [Contributing Expectations](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md), [Contributing packages](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_PACKAGES.md), or [Contribute to Great Expectations documentation](https://github.com/great-expectations/great_expectations/tree/develop/docs), and please don't be shy with questions.
