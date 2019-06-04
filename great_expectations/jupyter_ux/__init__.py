from IPython.core.display import display, HTML

def set_data_source(context, data_source_type):
    data_source_name = None

    configured_datasources = [datasource['name'] for datasource in context.list_datasources() if
                                     datasource['type'] == data_source_type]
    if len(configured_datasources) == 0:
        display(HTML("""
<p>
No {0:s} data sources found in the great_expectations.yml of your project.
</p>

<p>
If you did not create the data source during init, here is how to add it now: <a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">How To Add a Data Source</a>
</p>
""".format(data_source_type)))
    elif len(configured_datasources) > 1:
        display(HTML("""
<p>
Found more than one {0:s} data source in the great_expectations.yml of your project:
<b>{1:s}</b>
</p>
<p>
Uncomment the next cell and set data_source_name to one of these names.
</p>
""".format(data_source_type, ','.join(configured_datasources))))
    else:
        data_source_name = configured_datasources[0]
        display(HTML("Will be using this {0:s} data source from your project's great_expectations.yml: <b>{1:s}</b>".format(data_source_type, data_source_name)))

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
