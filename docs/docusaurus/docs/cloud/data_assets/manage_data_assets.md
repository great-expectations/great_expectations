---
sidebar_label: 'Manage Data Assets'
title: 'Manage Data Assets'
description: Create and manage Data Assets in GX Cloud.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

A Data Asset is a collection of records that you create when you connect to your Data Source. When you connect to your Data Source, you define a minimum of one Data Asset. You use these Data Assets to create the Batch Requests that select the data that is provided to your Expectations.

To learn more about Data Assets, see [Data Asset](../../terms/data_asset.md).

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud).

- You have [set up GX Cloud](../set_up_gx_cloud.md) including setting the `GX_CLOUD_SNOWFLAKE_PASSWORD` environment variable, and the GX Agent is running. 

- You have a [Snowflake account](https://docs.snowflake.com/en/user-guide-admin) with USAGE privileges on the table, database, and schema you are validating, and you know your password.


## Create a Data Asset

Create a Data Asset to define the data you want GX Cloud to access. Currently, the GX Cloud user interface is configured for Snowflake. To connect to Data Assets on another Data Source, see [Connect to a Data Source](https://deploy-preview-8760.docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_lp) in the GX OSS documentation. 

1. In GX Cloud, click **Data Assets** > **New Asset**.

2. Click the **New Data Source** tab and then select **Snowflake**.

3. Enter a meaningful name for the Data Asset in the **Data Source name** field.

4. Optional. To use a connection string to connect to a Data Source, click the **Use connection string** selector, enter a connection string, and then move to step 6. 

5. Complete the following fields:

    - **Username**: Enter your Snowflake username.

    - **Account identifier**: Enter your Snowflake account or locator information. The locator value must include the geographical region. For example, `us-east-1`. To locate these values see [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier).

    - **Password/environment variable**: Enter `${GX_CLOUD_SNOWFLAKE_PASSWORD}`. If you haven't set this variable, see [Set up GX Cloud](../set_up_gx_cloud.md).

    - **Database**: Enter the name of the Snowflake database where the data you want to validate is stored.
 
    - **Schema**: Enter the name of the schema for the Snowflake database where the data you want to validate is stored.

    - **Warehouse**: Enter the name of the Snowflake database warehouse.

    - **Role**: Enter your Snowflake role.

6. Optional. Make the following changes:

    - Clear **Create temp table** if you don't want to create a temporary database table. Temporary database tables store data temporarily and can improve performance by making queries run faster.

    - Select **Test connection** if you don't want to test the Data Asset connection. Testing the connection to the Data Asset is a preventative measure that makes sure the connection configuration is correct. This verification can help you avoid errors and can reduce troubleshooting downtime.

7. Click **Continue**.

8. Select **Table Asset** or **Query Asset** and complete the following fields:

    - **Table name**: When **Table Asset** is selected, enter a name for the table you're creating in the Data Asset.
    
    - **Data Asset name**: Enter a name for the Data Asset. Data Asset names must be unique. If you use the same name for multiple Data Assets, each Data Asset must be associated with a unique Data Source.

    - **Query**: When **Query Asset** is selected, enter the query that you want to run on the table. 

9. Select the **Complete Asset** tab to create Expectations and run validations on the entire Data Asset, or select the **Batches** tab to create Expectations and run validations on subsets of Data Asset records and then complete the following fields:

    - **Split Data Asset by** - Select **Year** to partition Data Asset records by year, select **Year - Month** to partition Data Asset records by year and month, or select **Year - Month - Day** to partition Data Asset records by year, month, and day.

    - **Column of datetime type** - Enter the name of the column containing the date and time data.

10. Optional. Select **Add Data Asset** to add additional tables or queries and repeat steps 8 and 9.

11. Click **Finish**.

12. Create an Expectation. See [Create an Expectation](/docs/cloud/expectations/manage_expectations#create-an-expectation).

## View Data Asset metrics

Data Asset metrics provide you with insight into the data you can use for your data validations. 

1. In GX Cloud, click **Data Assets** and then select a Data Asset in the **Data Assets** list.

2. Click the **Overview** tab.

3. Select one of the following options: 

    - If you have not previously generated Data Asset metrics, click **Fetch Metrics**. 

    - If you previously generated Data Asset metrics, click **Refresh** to refresh the metrics.

### Available Data Asset metrics

The following table lists the available Data Asset metrics.

| Column                                   | Description                                               | 
|------------------------------------------|-----------------------------------------------------------|
| **Row Count**                            | The number of rows within a Data Asset.                   | 
| **Column**                               | A column within your Data Asset.                          | 
| **Type**                                 | The data storage type in the Data Asset column.           | 
| **Min**                                  | For numeric columns the lowest value in the column.       | 
| **Max**                                  | For numeric columns, the highest value in the column.     | 
| **Mean**                                 | For numeric columns, the average value with the column.<br/> This is determined by dividing the sum of all values in the Data Asset by the number of values.  |
| **Median**                                 | For numeric columns, the value in the middle of a data set.<br/> 50% of the data within the Data Asset has a value smaller or equal to the median, and 50% of the data within the Data Asset has a value that is higher or equal to the median.  |
| **Null %**                                | The percentage of missing values in a column.             |

## Add an Expectation to a Data Asset column

When you create an Expectation after fetching metrics for a Data Asset, the column names and some values are autopopulated for you and this can simplify the creation of new Expectations. Data Asset Metrics can also help you determine what Expectations might be useful and how they should be configured. When you create new Expectations after fetching Data Asset Metrics, you can add them to an existing Expectation Suite, or you can create a new Expectation Suite and add the Expectations to it. 

1. In GX Cloud, click **Data Assets** and then select a Data Asset in the **Data Assets** list.

2. Click the **Overview** tab.

3. Select one of the following options: 

    - If you have not previously generated Data Asset metrics, click **Fetch Metrics**. 

    - If you previously generated Data Asset metrics, click **Refresh** to refresh the metrics.

4. Click **New Expectation**.

5. Select one of the following options:

    - To add an Expectation to a new Expectation Suite, click the **New Suite** tab and then enter a name for the new Expectation Suite.

    - To add an Expectation to an existing Expectation Suite, click the **Existing Suite** tab and then select an existing Expectation Suite.

6. Select an Expectation type. See [Available Expectation types](/docs/cloud/expectations/manage_expectations#available-expectation-types).

7. Complete the fields in the **Create Expectation** pane.

8. Click **Save** to add the Expectation, or click **Save & Add More** to add additional Expectations.


## Add a Data Asset to an Existing Data Source

Additional Data Assets can only be added to an existing Snowflake Data Source.

1. In GX Cloud, click **Data Assets** and then select **New Data Asset**.

2. Click the **Existing Data Source** tab and then select a Snowflake Data Source.

3. Click **Add another Data Asset**.

4. Select **Table Asset** or **Query Asset** and complete the following fields:

    - **Asset name**: Enter a name for the Data Asset. Data Asset names must be unique. If you use the same name for multiple Data Assets, each Data Asset must be associated with a unique Data Source.

    - **Table name**: When **Table Asset** is selected, enter a name for the table you're creating in the Data Asset.

    - **Query**: When **Query Asset** is selected, enter the query that you want to run on the table. 

5. Optional. Select **Add another Data Asset** to add additional tables or queries and repeat step 4.

6. Click **Finish**.


## Edit a Data Asset

1. In Jupyter Notebook, run the following code to import the `great_expectations` module and the existing Data Context:

    ```python title="Jupyter Notebook"
    import great_expectations as gx
    context = gx.get_context()
    ```

2. Run the following code to retrieve the Data Source:

    ```python title="Jupyter Notebook"
    datasource = context.get_datasource("<data_source_name>")
    ```

3. Edit the Data Asset settings. For example, run the following code to change the name of the Data Source:

    ```python title="Jupyter Notebook"
    datasource.name = "<new_data_source_name>"
    ```

    To review the Data Asset parameters that you can add or edit, see the [GX API documentation](https://deploy-preview-8760.docs.greatexpectations.io/docs/reference/api_reference).

 4. Run the following code to save your changes:

    ```python title="Jupyter Notebook"
    context.sources.update_snowflake(datasource)
    ```

## Delete a Data Asset

1. In GX Cloud, click **Settings** > **Datasources**.

2. Click **Delete** for the Data Source and the associated Data Assets you want to delete.

3. Click **Delete**.

