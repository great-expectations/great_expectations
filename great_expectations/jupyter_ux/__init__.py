"Utility functions for working with great_expectations within jupyter notebooks or jupyter lab.\n"
import logging
import sys
from datetime import datetime

import pandas as pd
import tzlocal
from IPython.display import HTML, display
from packaging import version

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
if version.parse(pd.__version__) <= version.parse("1.0.0"):
    pd.set_option("display.max_colwidth", (-1))
else:
    pd.set_option("display.max_colwidth", None)
from great_expectations.render.renderer import (
    ExpectationSuiteColumnSectionRenderer,
    ProfilingResultsColumnSectionRenderer,
    ValidationResultsColumnSectionRenderer,
)
from great_expectations.render.view import DefaultJinjaSectionView


def set_data_source(context, data_source_type=None):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "\n    TODO: Needs a docstring and tests.\n    "
    data_source_name = None
    if not data_source_type:
        configured_datasources = [
            datasource for datasource in context.list_datasources()
        ]
        if len(configured_datasources) == 0:
            display(
                HTML(
                    '\n<p>\nNo data sources found in the great_expectations.yml of your project.\n</p>\n\n<p>\nIf you did not create the data source during init, here is how to add it now: <a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">How To Add a Data Source</a>\n</p>\n'
                )
            )
        elif len(configured_datasources) > 1:
            display(
                HTML(
                    "\n<p>\nFound more than one data source in the great_expectations.yml of your project:\n<b>{1:s}</b>\n</p>\n<p>\nUncomment the next cell and set data_source_name to one of these names.\n</p>\n".format(
                        data_source_type,
                        ",".join(
                            [
                                datasource["name"]
                                for datasource in configured_datasources
                            ]
                        ),
                    )
                )
            )
        else:
            data_source_name = configured_datasources[0]["name"]
            display(
                HTML(
                    "Will be using this data source from your project's great_expectations.yml: <b>{:s}</b>".format(
                        data_source_name
                    )
                )
            )
    else:
        configured_datasources = [
            datasource["name"]
            for datasource in context.list_datasources()
            if (datasource["type"] == data_source_type)
        ]
        if len(configured_datasources) == 0:
            display(
                HTML(
                    '\n<p>\nNo {:s} data sources found in the great_expectations.yml of your project.\n</p>\n\n<p>\nIf you did not create the data source during init, here is how to add it now: <a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">How To Add a Data Source</a>\n</p>\n'.format(
                        data_source_type
                    )
                )
            )
        elif len(configured_datasources) > 1:
            display(
                HTML(
                    "\n<p>\nFound more than one {:s} data source in the great_expectations.yml of your project:\n<b>{:s}</b>\n</p>\n<p>\nUncomment the next cell and set data_source_name to one of these names.\n</p>\n".format(
                        data_source_type, ",".join(configured_datasources)
                    )
                )
            )
        else:
            data_source_name = configured_datasources[0]
            display(
                HTML(
                    "Will be using this {:s} data source from your project's great_expectations.yml: <b>{:s}</b>".format(
                        data_source_type, data_source_name
                    )
                )
            )
    return data_source_name


def setup_notebook_logging(logger=None, log_level=logging.INFO):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Set up the provided logger for the GE default logging configuration.\n\n    Args:\n        logger - the logger to configure\n    "

    def posix2local(timestamp, tz=tzlocal.get_localzone()):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Seconds since the epoch -> local time as an aware datetime object."
        return datetime.fromtimestamp(timestamp, tz)

    class Formatter(logging.Formatter):
        def converter(self, timestamp):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            return posix2local(timestamp)

        def formatTime(self, record, datefmt=None):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
    chandler.setFormatter(
        Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S%z")
    )
    logger.addHandler(chandler)
    logger.setLevel(log_level)
    logger.info(
        f"Great Expectations logging enabled at {log_level} level by JupyterUX module."
    )


def show_available_data_asset_names(context, data_source_name=None) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "List asset names found in the current context."
    styles = "\n    <style type='text/css'>\n    ul.data-assets {\n        margin-top: 0px;\n    }\n    ul.data-assets li {\n        line-height: 1.2em;\n        list-style-type: circle;\n    }\n    ul.data-assets li span.expectation-suite {\n        background: #ddd;\n    }\n    </style>\n    "
    print("Inspecting your data sources. This may take a moment...")
    expectation_suite_keys = context.list_expectation_suites()
    datasources = context.list_datasources()
    html = ""
    for datasource in datasources:
        if data_source_name and (datasource["name"] != data_source_name):
            continue
        html += "<h2 style='margin: 0'>Datasource: {:s} ({:s})</h2>".format(
            datasource["name"], datasource["class_name"]
        )
        ds = context.get_datasource(datasource["name"])
        generators = ds.list_batch_kwargs_generators()
        for generator_info in generators:
            html += "batch_kwargs_generator: {:s} ({:s})".format(
                generator_info["name"], generator_info["class_name"]
            )
            generator = ds.get_batch_kwargs_generator(generator_info["name"])
            mystery_object = generator.get_available_data_asset_names()
            if isinstance(mystery_object, dict) and ("names" in mystery_object.keys()):
                data_asset_names = sorted(name[0] for name in mystery_object["names"])
            elif isinstance(mystery_object, list):
                data_asset_names = sorted(mystery_object)
            else:
                data_asset_names = []
            if len(data_asset_names) > 0:
                html += "<h3 style='margin: 0.2em 0'>Data Assets Found:</h3>"
                html += styles
                html += "<ul class='data-assets'>"
                for data_asset_name in data_asset_names:
                    html += f"<li>{data_asset_name:s}</li>"
                    data_asset_expectation_suite_keys = [
                        es_key
                        for es_key in expectation_suite_keys
                        if (
                            (es_key.data_asset_name.datasource == datasource["name"])
                            and (
                                es_key.data_asset_name.generator
                                == generator_info["name"]
                            )
                            and (
                                es_key.data_asset_name.generator_asset
                                == data_asset_name
                            )
                        )
                    ]
                    if len(data_asset_expectation_suite_keys) > 0:
                        html += "<ul>"
                        for es_key in data_asset_expectation_suite_keys:
                            html += "<li><span class='expectation-suite'>Expectation Suite</span>: {:s}</li>".format(
                                es_key.expectation_suite_name
                            )
                        html += "</ul>"
                html += "</ul>"
            else:
                display(
                    HTML(
                        '<p>No data assets found in this data source.</p>\n<p>Read about how batch kwargs generators derive data assets from data sources:\n<a href="https://great-expectations.readthedocs.io/en/latest/how_to_add_data_source.html">Data assets</a>\n</p>'
                    )
                )
        display(HTML(html))


bootstrap_link_element = '<link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">'
cooltip_style_element = "<style type=\"text/css\">\n.cooltip {\n    display:inline-block;\n    position:relative;\n    text-align:left;\n}\n\n.cooltip .top {\n    min-width:200px;\n    top:-6px;\n    left:50%;\n    transform:translate(-50%, -100%);\n    padding:10px 20px;\n    color:#FFFFFF;\n    background-color:#222222;\n    font-weight:normal;\n    font-size:13px;\n    border-radius:8px;\n    position:absolute;\n    z-index:99999999;\n    box-sizing:border-box;\n    box-shadow:0 1px 8px rgba(0,0,0,0.5);\n    display:none;\n}\n\n.cooltip:hover .top {\n    display:block;\n}\n\n.cooltip .top i {\n    position:absolute;\n    top:100%;\n    left:50%;\n    margin-left:-12px;\n    width:24px;\n    height:12px;\n    overflow:hidden;\n}\n\n.cooltip .top i::after {\n    content:'';\n    position:absolute;\n    width:12px;\n    height:12px;\n    left:50%;\n    transform:translate(-50%,-50%) rotate(45deg);\n    background-color:#222222;\n    box-shadow:0 1px 8px rgba(0,0,0,0.5);\n}\n</style>\n"


def _render_for_jupyter(view, include_styling, return_without_displaying):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    if include_styling:
        html_to_display = (bootstrap_link_element + cooltip_style_element) + view
    else:
        html_to_display = view
    if return_without_displaying:
        return html_to_display
    else:
        display(HTML(html_to_display))


def display_column_expectations_as_section(
    expectation_suite, column, include_styling=True, return_without_displaying=False
):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    'This is a utility function to render all of the Expectations in an ExpectationSuite with the same column name as an HTML block.\n\n    By default, the HTML block is rendered using ExpectationSuiteColumnSectionRenderer and the view is rendered using DefaultJinjaSectionView.\n    Therefore, it should look exactly the same as the default renderer for build_docs.\n\n    Example usage:\n    exp = context.get_expectation_suite("notable_works_by_charles_dickens", "BasicDatasetProfiler")\n    display_column_expectations_as_section(exp, "Type")\n    '
    column_expectation_list = [
        e
        for e in expectation_suite.expectations
        if (("column" in e.kwargs) and (e.kwargs["column"] == column))
    ]
    document = (
        ExpectationSuiteColumnSectionRenderer()
        .render(column_expectation_list)
        .to_json_dict()
    )
    view = DefaultJinjaSectionView().render({"section": document, "section_loop": 1})
    return _render_for_jupyter(view, include_styling, return_without_displaying)


def display_profiled_column_evrs_as_section(
    evrs, column, include_styling=True, return_without_displaying=False
):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    'This is a utility function to render all of the EVRs in an ExpectationSuite with the same column name as an HTML block.\n\n    By default, the HTML block is rendered using ExpectationSuiteColumnSectionRenderer and the view is rendered using DefaultJinjaSectionView.\n    Therefore, it should look exactly the same as the default renderer for build_docs.\n\n    Example usage:\n    display_column_evrs_as_section(exp, "my_column")\n\n    WARNING: This method is experimental.\n    '
    column_evr_list = [
        e
        for e in evrs.results
        if (
            ("column" in e.expectation_config.kwargs)
            and (e.expectation_config.kwargs["column"] == column)
        )
    ]
    document = (
        ProfilingResultsColumnSectionRenderer().render(column_evr_list).to_json_dict()
    )
    view = DefaultJinjaSectionView().render(
        {"section": document, "section_loop": {"index": 1}}
    )
    return _render_for_jupyter(view, include_styling, return_without_displaying)


def display_column_evrs_as_section(
    evrs, column, include_styling=True, return_without_displaying=False
):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "\n    Display validation results for a single column as a section.\n\n    WARNING: This method is experimental.\n    "
    column_evr_list = [
        e
        for e in evrs.results
        if (
            ("column" in e.expectation_config.kwargs)
            and (e.expectation_config.kwargs["column"] == column)
        )
    ]
    document = (
        ValidationResultsColumnSectionRenderer().render(column_evr_list).to_json_dict()
    )
    view = DefaultJinjaSectionView().render(
        {"section": document, "section_loop": {"index": 1}}
    )
    return _render_for_jupyter(view, include_styling, return_without_displaying)


logger = logging.getLogger("great_expectations")
setup_notebook_logging(logger)
