"""Utility functions for working with great_expectations within jupyter notebooks or jupyter lab.
"""

import logging
import sys

import great_expectations.render as render
from datetime import datetime

import tzlocal
from IPython.core.display import display, HTML


def set_data_source(context, data_source_type=None):
    """
    TODO: Needs a docstring and tests.
    """

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
    """Set up the provided logger for the GE default logging configuration.

    Args:
        logger - the logger to configure
    """

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
        logger = logging.getLogger("great_expectations")
    chandler = logging.StreamHandler(stream=sys.stdout)
    chandler.setLevel(logging.DEBUG)
    # chandler.setFormatter(Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S%z"))
    chandler.setFormatter(Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S%z"))
    logger.addHandler(chandler)
    logger.setLevel(logging.INFO)
    logger.info("Great Expectations logging enabled at INFO level by JupyterUX module.")
    #
    # # Filter warnings
    # import warnings
    # warnings.filterwarnings('ignore')


def list_available_data_asset_names(context, data_source_name=None):
    """
    TODO: Needs a docstring and tests.
    """

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
                    print('    generator_asset: {0:s}'. \
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

bootstrap_link_element = """<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">"""
cooltip_style_element = """<style type="text/css">
.cooltip {
    display:inline-block;
    position:relative;
    text-align:left;
}

.cooltip .top {
    min-width:200px; 
    top:-6px;
    left:50%;
    transform:translate(-50%, -100%);
    padding:10px 20px;
    color:#FFFFFF;
    background-color:#222222;
    font-weight:normal;
    font-size:13px;
    border-radius:8px;
    position:absolute;
    z-index:99999999;
    box-sizing:border-box;
    box-shadow:0 1px 8px rgba(0,0,0,0.5);
    display:none;
}

.cooltip:hover .top {
    display:block;
}

.cooltip .top i {
    position:absolute;
    top:100%;
    left:50%;
    margin-left:-12px;
    width:24px;
    height:12px;
    overflow:hidden;
}

.cooltip .top i::after {
    content:'';
    position:absolute;
    width:12px;
    height:12px;
    left:50%;
    transform:translate(-50%,-50%) rotate(45deg);
    background-color:#222222;
    box-shadow:0 1px 8px rgba(0,0,0,0.5);
}
</style>
"""


def display_column_expectations_as_section(
    expectation_suite,
    column,
    section_renderer=render.renderer.column_section_renderer.ExpectationSuiteColumnSectionRenderer,
    view_renderer=render.view.view.DefaultJinjaSectionView,
    include_styling=True,
    return_without_displaying=False,
):
    """This is a utility function to render all of the Expectations in an ExpectationSuite with the same column name as an HTML block.

    By default, the HTML block is rendered using ExpectationSuiteColumnSectionRenderer and the view is rendered using DefaultJinjaSectionView.
    Therefore, it should look exactly the same as the default renderer for build_docs. 

    Example usage:
    exp = context.get_expectation_suite("notable_works_by_charles_dickens", "BasicDatasetProfiler")
    display_column_expectations_as_section(exp, "Type")
    """

    #TODO: replace this with a generic utility function, preferably a method on an ExpectationSuite class
    column_expectation_list = [ e for e in expectation_suite["expectations"] if "column" in e["kwargs"] and e["kwargs"]["column"] == column ]

    #TODO: Handle the case where zero evrs match the column name

    document = render.renderer.ExpectationSuiteColumnSectionRenderer.render(column_expectation_list)
    view = render.view.DefaultJinjaSectionView.render(
        render.types.RenderedComponentContentWrapper(**{
            "section": document,
            "section_loop": {"index": 1},
        })
    )

    if include_styling:
        html_to_display = bootstrap_link_element+cooltip_style_element+view
    else:
        html_to_display = view

    if return_without_displaying:
        return html_to_display
    else:
        display(HTML(html_to_display))


def display_column_evrs_as_section(
    evrs,
    column,
    section_renderer=render.renderer.column_section_renderer.ProfilingResultsColumnSectionRenderer,
    view_renderer=render.view.view.DefaultJinjaSectionView,
    include_styling=True,
    return_without_displaying=False,
):
    """This is a utility function to render all of the EVRs in an ExpectationSuite with the same column name as an HTML block.

    By default, the HTML block is rendered using ExpectationSuiteColumnSectionRenderer and the view is rendered using DefaultJinjaSectionView.
    Therefore, it should look exactly the same as the default renderer for build_docs. 

    Example usage:
    display_column_evrs_as_section(exp, "my_column")
    """

    #TODO: replace this with a generic utility function, preferably a method on an ExpectationSuite class
    column_evr_list = [ e for e in evrs["results"] if "column" in e["expectation_config"]["kwargs"] and e["expectation_config"]["kwargs"]["column"] == column ]

    #TODO: Handle the case where zero evrs match the column name

    document = render.renderer.ProfilingResultsColumnSectionRenderer.render(column_evr_list)
    view = render.view.DefaultJinjaSectionView.render(
        render.types.RenderedComponentContentWrapper(**{
            "section": document,
            "section_loop": {"index": 1},
        })
    )

    if include_styling:
        html_to_display = bootstrap_link_element+cooltip_style_element+view
    else:
        html_to_display = view

    if return_without_displaying:
        return html_to_display
    else:
        display(HTML(html_to_display))


# When importing the jupyter_ux module, we set up a preferred logging configuration
logger = logging.getLogger("great_expectations")
setup_notebook_logging(logger)
