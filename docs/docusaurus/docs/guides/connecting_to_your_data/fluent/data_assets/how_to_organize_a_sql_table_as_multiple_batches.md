### 5. (Optional) Add a Splitter to the table to divide it into Batches

```python title="Python code"
table_asset.add_year_and_month_splitter(column_name="pickup_datetime")
```

:::tip Splitters and Batch Identifiers

When requesting data from a table Data Asset you can use the command `table_asset.batch_request_options_template()` to see how to specify your Batch Request.  This will include the Batch Identifier keys that your splitter has added to your table Data Asset.

::: 

### 6. (Optional) Add a Batch Sorter to the table

When requesting data, Batches are returned as a list.  By adding a sorter to your table Data Asset you can define the order in which Batches appear in that list.  This will allow you to request a specific Batch by its list index rather than by its Batch Identifiers.

```python title="Python code"
table_asset.add_sorters(["-year", "+month"])
```