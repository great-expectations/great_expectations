{/* Generated file. Do not edit by hand. Regenerate with: invoke docs-tables --sync */}

| Data source | Support tier | Criteria met | Notes |
| --- | --- | --- | --- |
| AlloyDB | Best effort | No criteria met | The datasource API contract is verified for this data source, but that criterion is not shown as met because no continuous-integration lane is declared for it; what is missing is that declaration, not test evidence. |
| Amazon Aurora PostgreSQL | Best effort | No criteria met | The datasource API contract is verified for this data source, but that criterion is not shown as met because no continuous-integration lane is declared for it; what is missing is that declaration, not test evidence. |
| Amazon S3 | Best effort | Datasource API contract |  |
| Azure Blob Storage | Best effort | No criteria met | The datasource API contract is verified for this data source, but that criterion is not shown as met because no continuous-integration lane is declared for it; what is missing is that declaration, not test evidence. |
| BigQuery | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract |  |
| Citus | Best effort | No criteria met | The datasource API contract is verified for this data source, but that criterion is not shown as met because no continuous-integration lane is declared for it; what is missing is that declaration, not test evidence. |
| ClickHouse | Tested | Expectation suite (with exceptions)<br/>Datasource API contract | Expectation suite: not run for column names that need quoting. Recorded reason: This dialect's SQLAlchemy/driver insert path keys each row by the sanitized bind-parameter name instead of the real column name for identifiers requiring quoting, raising a \`KeyError\` at insert time and leaving the table empty. An issue still needs to be filed for this defect. |
| Databricks (SQL) | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract |  |
| Google Cloud Storage | Best effort | Datasource API contract |  |
| Microsoft Fabric | Best effort | No criteria met | The datasource API contract is verified for this data source, but that criterion is not shown as met because no continuous-integration lane is declared for it; what is missing is that declaration, not test evidence.<br/>No continuous-integration lane exercises the real service: reaching it needs credentials this repository does not provision. |
| MySQL | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract |  |
| Neon | Best effort | No criteria met | The datasource API contract is verified for this data source, but that criterion is not shown as met because no continuous-integration lane is declared for it; what is missing is that declaration, not test evidence. |
| Oracle | Tested | Expectation suite<br/>Datasource API contract | Tested against Oracle 21c. 19c expected, not verified in CI. |
| Pandas | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract (with exceptions) | Datasource API contract, in-memory pandas DataFrames: not run for keeping a single saved entry after create-or-update, replacing an existing datasource on create-or-update and replacing a datasource's configuration on update. Recorded reason: PandasDatasource declares no field beyond name, type, identifier and assets, so no update of its configuration can be observed to have replaced anything. |
| PostgreSQL | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract |  |
| Redshift | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract |  |
| SingleStore | Tested | Expectation suite<br/>Datasource API contract |  |
| Snowflake | Tested | Expectation suite<br/>Datasource API contract |  |
| Spark | Tested | Expectation suite<br/>Datasource API contract |  |
| SQL Server | Tested | Expectation suite<br/>Datasource API contract |  |
| SQLite | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract |  |
| Trino | Fully supported | Every shipped expectation<br/>Expectation suite<br/>Datasource API contract |  |

Connection paths GX ships that no row above covers: Databricks File System paths read with Spark; Databricks File System paths read with pandas; Power BI semantic models; Spark in-memory DataFrames.
