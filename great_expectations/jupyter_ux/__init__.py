import json
import os
import logging
import great_expectations as ge
from datetime import datetime

import tzlocal
from IPython.core.display import display, HTML

def set_data_source(context, data_source_type=None):
    data_source_name = None

    if not data_source_type:
        configured_datasources = [datasource for datasource in context.list_datasources()]

        if len(configured_datasources) == 0:
            display(HTML("""
<p>
No data sources found in the great_expectations.yml of your project.
</p>

<p>
If you did not create the data source during init, here is how to add it now: <a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">How To Add a Data Source</a>
</p>
""".format(data_source_type)))
        elif len(configured_datasources) > 1:
            display(HTML("""
<p>
Found more than one data source in the great_expectations.yml of your project:
<b>{1:s}</b>
</p>
<p>
Uncomment the next cell and set data_source_name to one of these names.
</p>
""".format(data_source_type, ','.join([datasource['name'] for datasource in configured_datasources]))))
        else:
            data_source_name = configured_datasources[0]['name']
            display(HTML("Will be using this data source from your project's great_expectations.yml: <b>{0:s}</b>".format(data_source_name)))

    else:
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

def setup_notebook_logging(logger=None):
    def posix2local(timestamp, tz=tzlocal.get_localzone()):
        """Seconds since the epoch -> local time as an aware datetime object."""
        return datetime.fromtimestamp(timestamp, tz)

    class Formatter(logging.Formatter):
        def converter(self, timestamp):
            return posix2local(timestamp)

        def formatTime(self, record, datefmt=None):
            dt = self.converter(record.created)
            if datefmt:
                s = dt.strftime(datefmt)
            else:
                t = dt.strftime(self.default_time_format)
                s = self.default_msec_format % (t, record.msecs)
            return s

    if not logger:
        logger = logging.getLogger()
    chandler = logging.StreamHandler()
    chandler.setLevel(logging.DEBUG)
    chandler.setFormatter(Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S%z"))
    logger.addHandler(chandler)
    logger.setLevel(logging.ERROR)
    # logger.setLevel(logging.INFO)
    logging.debug("test")

    # Filter warnings
    import warnings
    warnings.filterwarnings('ignore')

def list_available_data_asset_names(context, data_source_name=None):
    datasources = context.list_datasources()
    for datasource in datasources:
        if data_source_name and datasource['name'] != data_source_name:
            continue
        print('data_source: {0:s} ({1:s})'.format(datasource['name'], datasource['type']))
        ds = context.get_datasource(datasource['name'])
        generators = ds.list_generators()
        for generator_info in generators:
            print('  generator_name: {0:s} ({1:s})'.format(generator_info['name'], generator_info['type']))
            generator = ds.get_generator(generator_info['name'])
            data_asset_names = generator.get_available_data_asset_names()
            if len(data_asset_names) > 0:
                for data_asset_name in data_asset_names:
                    # print('    data asset: {0:s}. Full name: {1:s}/{2:s}/{0:s}'. \
                    print('    generator_asset: {0:s}. (Use this as an arg to get_batch)'. \
                    format(data_asset_name))
            else:
                display(HTML("""
                <p>
                No data assets found in this data source.
                </p>
                <p>
                Read about how generators derive data assets from data sources: <a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">Data assets</a>
                </p>
                            """))

    #TODO: add expectation suite names (existing)