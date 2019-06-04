from IPython.core.display import display, HTML

def set_data_source(context, data_source_type):
    data_source_name = None

    configured_pandas_datasources = [datasource['name'] for datasource in context.list_datasources() if
                                     datasource['type'] == data_source_type]
    if len(configured_pandas_datasources) == 0:
        display(HTML("""
<p>
No pandas data sources found in the great_expectations.yml of your project.
</p>

<p>
If you did not create the data source during init, here is how to add it now: <a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">How To Add a Data Source</a>
</p>
"""))
    elif len(configured_pandas_datasources) > 1:
        display(HTML("""
<p>
Found more than one pandas data source in the great_expectations.yml of your project:
<b>{0:s}</b>
</p>
<p>
Uncomment the next cell and set data_source_name to one of these names.
</p>
""".format(','.join(configured_pandas_datasources))))
    else:
        data_source_name = configured_pandas_datasources[0]
        display(HTML("Will be using this pandas data source from your project's great_expectations.yml: <b>{0:s}</b>".format(
            data_source_name)))

    return data_source_name

def list_available_data_asset_names(context, data_source_name):
    available_data_assets = context.list_available_data_asset_names(datasource_names=[data_source_name])

    if len(available_data_assets) == 1 and\
        len(available_data_assets[0]['generators']) == 1:
        if len(available_data_assets[0]['generators'][0]['available_data_asset_names']) > 0:
            print(available_data_assets[0]['generators'][0]['available_data_asset_names'])
        else:
            display(HTML("""
<p>
No data assets found in this data source.
</p>
<p>
Read about how generators derive data assets from data sources: <a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">Data assets</a>
</p>
            """))
    elif len(available_data_assets) > 1:
        print(available_data_assets)
