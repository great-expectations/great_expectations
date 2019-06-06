# Content Block Types

- header

``` json
{
    "content_block_type": "header",
    "header": header (string)
}
```

- value_list

``` json
{
    "content_block_type": "value_list",
    "value_list": [ value (string) ]
}
```

- bullet_list

``` json
{
    "content_block_type": "bullet_list",
    "classes": classes (string)
}

- table
For a table, the header row and each item in table_rows should have the same number of elements

``` json
{
    "content_block_type": "table",
    "table_rows": [ [table_entry] ]
}

- graph

``` json
{
    "content_block_type": "graph",
    "graph": [ vega_graph_specification (json) ]
}
```
