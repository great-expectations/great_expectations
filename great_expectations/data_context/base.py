import os
import json
import logging
import yaml
import sys
from glob import glob

from great_expectations.version import __version__
from great_expectations.dataset import PandasDataset
from great_expectations import read_csv, render
from IPython.display import display
import ipywidgets as widgets
from urllib.parse import urlparse

from .sqlalchemy_source import SqlAlchemyDataSource, DBTDataSource
from .pandas_source import PandasCSVDataSource

logger = logging.getLogger(__name__)


class ExpectationExplorer(object):
    def __init__(self):
        self.expectation_widgets = {}
        self.expectation_kwarg_field_names = {
            'expect_column_values_to_be_unique': ['mostly'],
            'expect_column_unique_value_count_to_be_between': ['min_value', 'max_value'],
            'expect_column_values_to_be_in_set': ['value_set', 'mostly'],
            'expect_column_to_exist': ['column_index'],
            'expect_column_values_to_not_be_null': ['mostly'],
            'expect_column_values_to_be_null': ['mostly'],
            'expect_column_values_to_not_be_in_set': ['value_set', 'mostly'],
            'expect_column_values_to_match_regex': ['regex', 'mostly'],
            'expect_column_values_to_not_match_regex': ['regex', 'mostly'],
            'expect_column_values_to_match_regex_list': ['regex_list', 'match_on', 'mostly'],
            'expect_column_values_to_not_match_regex_list': ['regex_list', 'mostly'],
            'expect_column_values_to_match_strftime_format': ['strftime_format', 'mostly'],
            'expect_column_values_to_be_json_parseable': ['mostly'],
            'expect_column_values_to_match_json_schema': ['json_schema', 'mostly'],
            'expect_column_value_lengths_to_equal': ['value', 'mostly'],
            'expect_column_value_lengths_to_be_between': ['min_value', 'max_value', 'mostly'],
            'expect_column_values_to_be_between': ['min_value', 'max_value', 'allow_cross_type_comparisons', 'parse_strings_as_datetimes', 'output_strftime_format', 'mostly'],
            'expect_column_max_to_be_between': ['min_value', 'max_value', 'parse_strings_as_datetimes', 'output_strftime_format'],
            'expect_column_min_to_be_between': ['min_value', 'max_value', 'parse_strings_as_datetimes', 'output_strftime_format'],
            'expect_table_row_count_to_equal': ['value'],
            'expect_table_row_count_to_be_between': ['min_value', 'max_value'],
            'expect_table_columns_to_match_ordered_list': ['column_list'],
            'expect_column_proportion_of_unique_values_to_be_between': ['min_value', 'max_value'],
            'expect_column_values_to_be_dateutil_parseable': ['mostly'],
            'expect_column_values_to_be_increasing': ['strictly', 'parse_strings_as_datetimes', 'mostly'],
            'expect_column_values_to_be_decreasing': ['strictly', 'parse_strings_as_datetimes', 'mostly'],
            'expect_column_median_to_be_between': ['min_value', 'max_value'],
            'expect_column_mean_to_be_between': ['min_value', 'max_value'],
            'expect_column_stdev_to_be_between': ['min_value', 'max_value'],
            'expect_column_kl_divergence_to_be_less_than': ['partition_object', 'threshold', 'internal_weight_holdout', 'tail_weight_holdout'],
            'expect_column_sum_to_be_between': ['min_value', 'max_value'],
            'expect_column_most_common_value_to_be_in_set': ['value_set', 'ties_okay'],
            'expect_column_pair_values_to_be_equal': ['ignore_row_if'],
            'expect_column_pair_values_A_to_be_greater_than_B': ['or_equal', 'allow_cross_type_comparisons', 'ignore_row_if'],
            'expect_column_pair_values_to_be_in_set': ['value_pairs_set', 'ignore_row_if'],
            ####
            'expect_column_values_to_be_of_type': ['type_', 'mostly'],
            #             'expect_column_values_to_be_of_semantic_type': [],
        }
        # debug_view is for debugging ipywidgets callback functions
        self.debug_view = widgets.Output(layout={'border': '3 px solid pink'})

    def update_result(self, new_result, expectation_type, column=None):
        new_success_value = new_result.get('success')
        new_result_widgets = self.generate_expectation_result_detail_widgets(
            new_result['result'], expectation_type)
        new_border_color = 'green' if new_success_value else 'red'

        if column:
            self.expectation_widgets[column][expectation_type][
                'success'].value = f'<span><strong>Success: </strong>{str(new_success_value)}</span>'
            self.expectation_widgets[column][expectation_type]['result_detail_widget']\
                .children = new_result_widgets
            self.expectation_widgets[column][expectation_type]['editor_widget']\
                .layout\
                .border = f'2px solid {new_border_color}'
        else:
            self.expectation_widgets['non_column_expectations'][expectation_type][
                'success'].value = f'<span><strong>Success: </strong>{str(new_success_value)}</span>'
            self.expectation_widgets['non_column_expectations'][expectation_type]['result_detail_widget']\
                .children = new_result_widgets
            self.expectation_widgets['non_column_expectations'][expectation_type]['editor_widget']\
                .layout\
                .border = f'2px solid {new_border_color}'

    def get_expectation_state(self, expectation_type, column=None):
        if column:
            column_expectations = self.expectation_widgets.get(column)
            if not column_expectations:
                return None
            return column_expectations.get(expectation_type)
        else:
            non_column_expectations = self.expectation_widgets.get(
                'non_column_expectations')
            if not non_column_expectations:
                return None
            return column_expectations.get(expectation_type)

    def set_expectation_state(self, expectation_type, expectation_state, column=None):
        if column:
            column_expectations = self.expectation_widgets.get(column, {})
            column_expectations[expectation_type] = expectation_state
            self.expectation_widgets[column] = column_expectations
        else:
            non_column_expectations = self.expectation_widgets.get(
                'non_column_expectations', {})
            non_column_expectations[expectation_type] = expectation_state
            self.expectation_widgets['non_column_expectations'] = non_column_expectations

    def update_expectation_state(self, ge_df, existing_expectation_state, expectation_validation_result):
        expectation_editor_widget = existing_expectation_state.get(
            'editor_widget')
        expectation_type = expectation_validation_result['expectation_config']['expectation_type']
        new_expectation_kwargs = expectation_validation_result['expectation_config']['kwargs']
        existing_expectation_kwarg_widgets = existing_expectation_state['kwargs']
        column = existing_expectation_kwarg_widgets.get('column')

        new_kwarg_widget_values = self.ge_kwargs_to_widget_values(
            new_expectation_kwargs)

        for kwarg_name, kwarg_value in new_kwarg_widget_values.items():
            if kwarg_name == 'column':
                continue
            existing_widget = existing_expectation_kwarg_widgets.get(
                kwarg_name)
            if existing_widget:
                if getattr(existing_widget, 'value', None):
                    existing_widget.value = kwarg_value
                else:
                    existing_expectation_kwarg_widgets[kwarg_name] = kwarg_value
            else:
                widget_generator = getattr(
                    self, f'generate_{kwarg_name}_widget', None)
                widget = widget_generator(ge_df=ge_df, expectation_type=expectation_type, **expectation_kwargs) if widget_generator \
                    else self.generate_expectation_kwarg_fallback_widget(expectation_kwarg_name=kwarg_name, **new_expectation_kwargs)
                existing_expectation_kwarg_widgets[kwarg_name] = widget
                expectation_editor_widget.children[0].children[1].children += (
                    widget,)

        self.update_result(new_result=expectation_validation_result,
                           expectation_type=expectation_type, column=column)
        return expectation_editor_widget

    # interconvert expectation kwargs
    def expectation_kwarg_widgets_to_ge_kwargs(self, kwarg_widgets):
        def kwarg_transformer(kwarg_key, kwarg_value):
            kwarg_value = kwarg_value.value if getattr(
                kwarg_value, 'value', None) else value
            transformers = {
                'value_set': lambda value_set_string: [item.strip() for item in value_set_string.split(',')]
            }
            return transformers.get(kwarg_key, lambda kwarg_value: kwarg_value)(kwarg_value)

        expectation_kwargs = kwarg_widgets.copy()

        for key, value in expectation_kwargs.items():
            expectation_kwargs[key] = kwarg_transformer(key, value)

        return expectation_kwargs

    def ge_kwargs_to_widget_values(self, ge_kwargs):
        def kwarg_transformer(kwarg_key, kwarg_value):
            transformers = {
                'value_set': lambda value_set_list: ', '.join(value_set_list)
            }
            return transformers.get(kwarg_key, lambda kwarg_value: kwarg_value)(kwarg_value)

        expectation_kwargs = ge_kwargs.copy()

        for key, value in expectation_kwargs.items():
            if not getattr(self, f'generate_{key}_widget', None):
                continue
            expectation_kwargs[key] = kwarg_transformer(key, value)

        return expectation_kwargs

    # widget generators for input fields
    def generate_mostly_widget(self, *, ge_df, mostly=1, expectation_type, column=None, **expectation_kwargs):
        mostly_widget = widgets.FloatSlider(
            value=mostly,
            min=0,
            max=1.0,
            step=0.01,
            description='mostly: ',
            continuous_update=True,
            orientation='horizontal',
            readout=True,
            readout_format='.2f'
        )

        def on_mostly_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])

            new_result = getattr(ge_df, expectation_type)(
                **ge_expectation_kwargs)
            with self.debug_view:
                display(new_result)
            self.update_result(new_result, expectation_type, column)

        mostly_widget.observe(on_mostly_change, names='value')
        return mostly_widget

    def generate_min_value_widget(self, *, ge_df, expectation_type, min_value=None, column=None, **expectation_kwargs):
        expectation_state = self.get_expectation_state(
            expectation_type, column) or {'kwargs': {}}
        min_value_widget = expectation_state['kwargs'].get('min_value')
        max_value_widget = expectation_state['kwargs'].get('max_value')

        if expectation_type == 'expect_column_unique_value_count_to_be_between':
            if min_value_widget:
                min_value_widget.value = min_value or int(-9e300)
            else:
                min_value_widget = widgets.BoundedIntText(
                    value=min_value or 0,
                    min=0,
                    description='min_value: ',
                    disabled=False
                )
            if not max_value_widget:
                max_value_widget = widgets.BoundedIntText(
                    description='max_value: ',
                    value=int(9e300),
                    max=int(9e300),
                    disabled=False
                )

        def on_min_value_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])
            new_result = getattr(ge_df, expectation_type)(
                **ge_expectation_kwargs)

            self.update_result(new_result, expectation_type, column)

        min_value_widget.observe(on_min_value_change, names='value')
        max_dl = widgets.link((max_value_widget, 'value'),
                              (min_value_widget, 'max'))

        expectation_state['kwargs']['min_value'] = min_value_widget
        expectation_state['kwargs']['max_value'] = max_value_widget
        self.set_expectation_state(expectation_type, expectation_state, column)

        return min_value_widget

    def generate_max_value_widget(self, *, ge_df, expectation_type, max_value=None, column=None, **expectation_kwargs):
        expectation_state = self.get_expectation_state(
            expectation_type, column) or {'kwargs': {}}
        min_value_widget = expectation_state['kwargs'].get('min_value')
        max_value_widget = expectation_state['kwargs'].get('max_value')

        if expectation_type == 'expect_column_unique_value_count_to_be_between':
            if max_value_widget:
                max_value_widget.value = max_value or int(9e300)
            else:
                max_value_widget = widgets.BoundedIntText(
                    value=max_value or int(9e300),
                    max=int(9e300),
                    description='max_value: ',
                    disabled=False
                )
            if not min_value_widget:
                min_value_widget = widgets.BoundedIntText(
                    min=0,
                    value=0,
                    description='min_value: ',
                    disabled=False
                )

        def on_max_value_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])
            new_result = getattr(ge_df, expectation_type)(
                **ge_expectation_kwargs)

            self.update_result(new_result, expectation_type, column)

        max_value_widget.observe(on_max_value_change, names='value')
        min_dl = widgets.link((min_value_widget, 'value'),
                              (max_value_widget, 'min'))

        expectation_state['kwargs']['min_value'] = min_value_widget
        expectation_state['kwargs']['max_value'] = max_value_widget
        self.set_expectation_state(expectation_type, expectation_state, column)

        return max_value_widget

    def generate_value_set_widget(self, *, ge_df, expectation_type, value_set, column, **expectation_kwargs):
        expectation_state = self.get_expectation_state(
            expectation_type, column)
        value_set_widget = widgets.Textarea(
            value=', '.join(value_set),
            placeholder='Please enter comma-separated set values.',
            description='value_set: ',
            disabled=False
        )

        def on_value_set_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])

            new_result = getattr(ge_df, expectation_type)(
                **ge_expectation_kwargs)

            self.update_result(new_result, expectation_type, column)

        value_set_widget.observe(on_value_set_change, names='value')
        return value_set_widget

    def generate_expectation_kwarg_fallback_widget(self, *, expectation_kwarg_name, **expectation_kwargs):
        expectation_kwarg_value = expectation_kwargs.get(
            expectation_kwarg_name)
        warning_message = widgets.HTML(
            value=f'<div><strong>Warning: </strong>Cannot find dynamic widget for expectation kwarg "{expectation_kwarg_name}". To change kwarg value, please call expectation again with the modified value.</div>'
        )
        static_widget = widgets.Textarea(value=str(
            expectation_kwarg_value), description=f'{expectation_kwarg_name}: ', disabled=True)
        return widgets.VBox([warning_message, static_widget])

    # widget generators for general info shared between all expectations
    def generate_column_widget(self, column, **kwargs):
        #         return widgets.HTML(value=f'<span><strong>Column: </strong>{column}</span>') if column else None
        return widgets.HTML(value=f'<div><style type<strong>Column: </strong>{column}</div>') if column else None

    def generate_expectation_type_widget(self, expectation_type):
        return widgets.HTML(value=f'<span><strong>Expectation Type: </strong>{expectation_type}</span>')

    def generate_success_widget(self, success):
        return widgets.HTML(value=f'<span><strong>Success: </strong>{str(success)}</span>')

    def generate_basic_expectation_info_box(self, expectation_type, success_widget, column=None):
        if column:
            return widgets.VBox(
                [
                    self.generate_column_widget(column=column),
                    self.generate_expectation_type_widget(expectation_type),
                    success_widget
                ],
                layout=widgets.Layout(margin='10px')
            )
        else:
            return widgets.VBox(
                [
                    self.generate_expectation_type_widget(expectation_type),
                    success_widget
                ],
                layout=widgets.Layout(margin='10px')
            )

    def generate_expectation_result_detail_widgets(self, result, expectation_type):
        result_detail_widgets = []

        for result_title, result_value in result.items():
            result_detail_widgets.append(
                widgets.HTML(
                    value="<span><strong>{0}: </strong>{1:.2f}</span>".format(result_title, result_value) if type(result_value) is float
                    else "<span><strong>{}: </strong>{}</span>".format(result_title, result_value)

                )
            )

        return result_detail_widgets

    # widget generator for complete expectation widget
    def create_expectation_widget(
            self,
            ge_df,
            expectation_validation_result
    ):
        expectation_type = expectation_validation_result['expectation_config']['expectation_type']
        expectation_kwargs = expectation_validation_result['expectation_config']['kwargs']
        column = expectation_kwargs.get('column')

        existing_expectation_state = self.get_expectation_state(
            expectation_type=expectation_type, column=column)
        if existing_expectation_state:
            return self.update_expectation_state(ge_df, existing_expectation_state, expectation_validation_result)

        # success_widget
        success = expectation_validation_result['success']
        success_widget = widgets.HTML(
            value=f'<span><strong>Success: </strong>{str(success)}</span>')

        # widget with basic info (column, expectation_type)
        basic_expectation_info_box = self.generate_basic_expectation_info_box(
            expectation_type, success_widget, column)

        # widget with result details
        result_detail_widget = widgets.VBox(children=self.generate_expectation_result_detail_widgets(
            expectation_validation_result["result"],
            expectation_type
        ))

        # accordion container for result_detail_widget
        result_detail_accordion = widgets.Accordion(
            children=[result_detail_widget])
        result_detail_accordion.set_title(0, 'Validation Result Details')
        result_detail_accordion.selected_index = None

        # create scaffold dict for expectation state
        expectation_state = {
            'success': success_widget,
            'result_detail_widget': result_detail_widget,
            'kwargs': {
            }
        }
        if column:
            expectation_state['kwargs']['column'] = column

        # collect widgets for kwarg inputs
        expectation_kwarg_input_widgets = []
        for expectation_kwarg_name in self.expectation_kwarg_field_names.get(expectation_type):
            widget_generator = getattr(
                self, f'generate_{expectation_kwarg_name}_widget', None)
            widget = widget_generator(ge_df=ge_df, expectation_type=expectation_type, **expectation_kwargs) if widget_generator \
                else self.generate_expectation_kwarg_fallback_widget(expectation_kwarg_name=expectation_kwarg_name, **expectation_kwargs)
            expectation_state['kwargs'][expectation_kwarg_name] = widget
            expectation_kwarg_input_widgets.append(widget)

        # container for kwarg input widgets
        expectation_kwarg_input_box = widgets.VBox(
            expectation_kwarg_input_widgets,
            layout=widgets.Layout(margin='10px')
        )

        # top-level widget container
        expectation_editor_widget = widgets.VBox(
            [
                widgets.HBox([basic_expectation_info_box,
                              expectation_kwarg_input_box]),
                result_detail_accordion
            ],
            layout=widgets.Layout(
                border=f'2px solid {"green" if success else "red"}',
                margin='5px'
            )
        )

        # complete expectation state scaffold
        expectation_state['editor_widget'] = expectation_editor_widget

        # set expectation state
        self.set_expectation_state(expectation_type, expectation_state, column)

        return expectation_editor_widget

    def edit_expectations(self):
        expectation_widget_list = []
        for expectation_collection in self.expectation_widgets.values():
            for expectation in expectation_collection.values():
                expectation_widget_list.append(
                    expectation.get('editor_widget'))
        for widget in expectation_widget_list:
            display(widget)


class DataContext(object):
    #TODO: update class documentation
    """A generic DataContext, exposing the base API including constructor with `options` parameter, list_datasets,
    and get_dataset.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, options=None, expectation_explorer=False, *args, **kwargs):
        self._expectation_explorer = expectation_explorer
        if expectation_explorer:
            self._expectation_explorer_manager = ExpectationExplorer()
        self.connect(options, *args, **kwargs)

    def connect(self, context_root_dir):
        # determine the "context root directory" - this is the parent of "great_expectations" dir

        if context_root_dir is None:
            raise Exception("the guessing logic not implemented yet!")
        # TODO: Revisit this logic to better at making real guesses
        # if os.path.isdir("../notebooks") and os.path.isdir("../../great_expectations"):
            #     self.context_root_directory = "../data_asset_configurations"
            # else:
            #     self.context_root_directory = "./great_expectations/data_asset_configurations"
        else:
            if os.path.isdir(os.path.join(context_root_dir, "great_expectations")):
                self.context_root_directory = context_root_dir
            else:
                self.context_root_directory = context_root_dir

        self.context_root_directory = os.path.abspath(self.context_root_directory)

        self.directory = os.path.join(self.context_root_directory, "great_expectations/data_asset_configurations")
        self.plugin_store_directory = os.path.join(self.context_root_directory, "great_expectations/plugins/store")
        sys.path.append(self.plugin_store_directory)

        # TODO: What if the project config file does not exist?
        # TODO: Should we merge the project config file with the global config file?
        with open(os.path.join(self.context_root_directory, ".great_expectations.yml"), "r") as data:
            self._project_config = yaml.safe_load(data) or {}

        self._load_evaluation_parameter_store()

        self._compiled = False

    def list_data_assets(self, datasource_name="default"):
        datasource = self._get_datasource(datasource_name)
        return datasource.list_data_assets()

    def get_data_asset(self, datasource_name="default", data_asset_name="None", *args, **kwargs):
        datasource = self._get_datasource(datasource_name)
        data_asset = datasource.get_data_asset(data_asset_name, *args, data_context=self, **kwargs)
        data_asset._initialize_expectations(self.get_data_asset_config(data_asset_name))
        return data_asset

    def _get_datasource(self, datasource_name):
        try:
            datasource_config = self._project_config["datasources"][datasource_name]
            datasource_type = datasource_config["type"]
            if datasource_type == "pandas":
                return PandasCSVDataSource(**datasource_config)

            elif datasource_type == "dbt":
                profile = datasource_config["profile"]
                return DBTDataSource(profile)

            elif datasource_type == "sqlalchemy":
                return SqlAlchemyDataSource(**datasource_config)
            else:
                raise ValueError(f"Unrecognized datasource type {datasource_type}")

        except KeyError:
            raise ValueError(f"Unable to load datasource {datasource_name} -- no configuration found or invalid configuration.")


    def _load_evaluation_parameter_store(self):

        # This is a trivial class that implements in-memory key value store.
        # We use it when user does not specify a custom class in the config file
        class MemoryEvaluationParameterStore(object):
            def __init__(self):
                self.dict = {}
            def get(self, run_id, name):
                if run_id in self.dict:
                    return self.dict[run_id][name] 
                else:
                    return {}
            def set(self, run_id, name, value):
                if not run_id in self.dict:
                    self.dict[run_id] = {}
                self.dict[run_id][name] = value
            def get_run_parameters(self, run_id):
                if run_id in self.dict:
                    return self.dict[run_id]
                else:
                    return {}

        # If user wishes to provide their own implementation for this key value store (e.g.,
        # Redis-based), they should specify the following in the project config file:
        #
        # evaluation_parameter_store:
        #   type: demostore
        #   config:  - this is optional - this is how we can pass kwargs to the object's c-tor
        #     param1: boo
        #     param2: bah
        #
        # Module called "demostore" must be found in great_expectations/plugins/store.
        # Class "GreatExpectationsEvaluationParameterStore" must be defined in that module.
        # The class must implement the following methods:
        # 1. def __init__(self, **kwargs)
        #
        # 2. def get(self, name)
        #
        # 3. def set(self, name, value)
        #
        # We will load the module dynamically
        try:
            config_block = self._project_config.get("evaluation_parameter_store")
            if not config_block or not config_block.get("type"):
                self._evaluation_parameter_store = MemoryEvaluationParameterStore()
            else:
                module_name = config_block.get("type")
                class_name = "GreatExpectationsEvaluationParameterStore"

                loaded_module = __import__(module_name, fromlist=[module_name])
                loaded_class = getattr(loaded_module, class_name)
                if config_block.get("config"):
                    self._evaluation_parameter_store = loaded_class(**config_block.get("config"))
                else:
                    self._evaluation_parameter_store = loaded_class()
        except Exception as err:
            logger.exception("Failed to load evaluation_parameter_store class")
            raise

    def list_data_asset_configs(self):
        root_path = self.directory
        result = [os.path.splitext(os.path.relpath(y, root_path))[0] for x in os.walk(root_path) for y in glob(os.path.join(x[0], '*.json'))]
        return result

    def get_data_asset_config(self, data_asset_name):
        config_file_path = os.path.join(self.directory, data_asset_name + '.json')
        if os.path.isfile(config_file_path):
            with open(os.path.join(self.directory, data_asset_name + '.json')) as json_file:
                return json.load(json_file)
        else:
            #TODO (Eugene): Would it be better to return None if the file does not exist? Currently this method acts as
            # get_or_create
            return {
                'data_asset_name': data_asset_name,
                'meta': {
                    'great_expectations.__version__': __version__
                },
                'expectations': [],
             }

    def save_data_asset_config(self, data_asset_config):
        data_asset_name = data_asset_config['data_asset_name']
        config_file_path = os.path.join(self.directory, data_asset_name + '.json')
        os.makedirs(os.path.split(config_file_path)[0], exist_ok=True)
        with open(config_file_path, 'w') as outfile:
            json.dump(data_asset_config, outfile)
        self._compiled = False

    def bind_evaluation_parameters(self, run_id, expectations_config):
        return self._evaluation_parameter_store.get_run_parameters(run_id)

    def register_validation_results(self, run_id, validation_results):
        if not self._compiled:
            self._compile()

        if "meta" not in validation_results or "data_asset_name" not in validation_results["meta"]:
            logger.warning("No data_asset_name found in validation results; evaluation parameters cannot be registered.")
            return
        elif validation_results["meta"]["data_asset_name"] not in self._compiled_parameters["data_assets"]:
            # This is fine; short-circuit since we do not need to register any results from this dataset.
            return
        else:
            data_asset_name = validation_results["meta"]["data_asset_name"]
        
        for result in validation_results['results']:
            # Unoptimized: loop over all results and check if each is needed
            expectation_type = result['expectation_config']['expectation_type']
            if expectation_type in self._compiled_parameters["data_assets"][data_asset_name]:
                # First, bind column-style parameters
                if (("column" in result['expectation_config']['kwargs']) and 
                    ("columns" in self._compiled_parameters["data_assets"][data_asset_name][expectation_type]) and 
                    (result['expectation_config']['kwargs']["column"] in self._compiled_parameters["data_assets"][data_asset_name][expectation_type]["columns"])):

                    column = result['expectation_config']['kwargs']["column"]
                    # Now that we have a small search space, invert logic, and look for the parameters in our result
                    for type_key, desired_parameters in self._compiled_parameters["data_assets"][data_asset_name][expectation_type]["columns"][column].items():
                        # value here is the set of desired parameters under the type_key
                        for desired_param in desired_parameters:
                            desired_key = desired_param.split(":")[-1]
                            if type_key == "result" and desired_key in result['result']:
                                self.store_validation_param(run_id, desired_param, result["result"][desired_key])
                            elif type_key == "details" and desired_key in result["result"]["details"]:
                                self.store_validation_param(run_id, desired_param, result["result"]["details"])
                            else:
                                logger.warning("Unrecognized key for parameter %s" % desired_param)
                
                # Next, bind parameters that do not have column parameter
                for type_key, desired_parameters in self._compiled_parameters["data_assets"][data_asset_name][expectation_type].items():
                    if type_key == "columns":
                        continue
                    for desired_param in desired_parameters:
                        desired_key = desired_param.split(":")[-1]
                        if type_key == "result" and desired_key in result['result']:
                            self.store_validation_param(run_id, desired_param, result["result"][desired_key])
                        elif type_key == "details" and desired_key in result["result"]["details"]:
                            self.store_validation_param(run_id, desired_param, result["result"]["details"])
                        else:
                            logger.warning("Unrecognized key for parameter %s" % desired_param)

    def store_validation_param(self, run_id, key, value):
        self._evaluation_parameter_store.set(run_id, key, value)

    def get_validation_param(self, run_id, key):
        return self._evaluation_parameter_store.get(run_id, key)

    def _compile(self):
        """Compiles all current expectation configurations in this context to be ready for reseult registration.
        
        Compilation only respects parameters with a URN structure beginning with urn:great_expectations:validations
        It splits parameters by the : (colon) character; valid URNs must have one of the following structures to be
        automatically recognized.

        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "columns" : column_name : "result": result_key
         [0]            [1]                 [2]              [3]              [4]              [5]              [6]          [7]         [8]        [9]
        
        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "columns" : column_name : "details": details_key
         [0]            [1]                 [2]              [3]              [4]              [5]              [6]          [7]         [8]        [9]

        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "result": result_key
         [0]            [1]                 [2]              [3]              [4]              [5]            [6]          [7]  

        "urn" : "great_expectations" : "validations" : data_asset_name : "expectations" : expectation_name : "details": details_key
         [0]            [1]                 [2]              [3]              [4]              [5]             [6]          [7]  

         Parameters are compiled to the following structure:
         {
             "raw": <set of all parameters requested>
             "data_assets": {
                 data_asset_name: {
                    expectation_name: {
                        "details": <set of details parameter values requested>
                        "result": <set of result parameter values requested>
                        column_name: {
                            "details": <set of details parameter values requested>
                            "result": <set of result parameter values requested>
                        }
                    }
                 }
             }
         }

        """

        # Full recompilation every time
        self._compiled_parameters = {
            "raw": set(),
            "data_assets": {}
        }

        known_assets = self.list_data_asset_configs()
        config_paths = [y for x in os.walk(self.directory) for y in glob(os.path.join(x[0], '*.json'))]

        for config_file in config_paths:
            config = json.load(open(config_file, 'r'))
            for expectation in config["expectations"]:
                for _, value in expectation["kwargs"].items():
                    if isinstance(value, dict) and '$PARAMETER' in value:
                        # Compile only respects parameters in urn structure beginning with urn:great_expectations:validations
                        if value["$PARAMETER"].startswith("urn:great_expectations:validations:"):
                            column_expectation = False
                            parameter = value["$PARAMETER"]
                            self._compiled_parameters["raw"].add(parameter)
                            param_parts = parameter.split(":")
                            try:
                                data_asset = param_parts[3]
                                expectation_name = param_parts[5]
                                if param_parts[6] == "columns":
                                    column_expectation = True
                                    column_name = param_parts[7]
                                    param_key = param_parts[8]
                                else:
                                    param_key = param_parts[6]
                            except IndexError:
                                logger.warning("Invalid parameter urn (not enough parts): %s" % parameter)

                            if data_asset not in known_assets:
                                logger.warning("Adding parameter %s for unknown data asset config" % parameter)

                            if data_asset not in self._compiled_parameters["data_assets"]:
                                self._compiled_parameters["data_assets"][data_asset] = {}

                            if expectation_name not in self._compiled_parameters["data_assets"][data_asset]:
                                self._compiled_parameters["data_assets"][data_asset][expectation_name] = {}

                            if column_expectation:
                                if "columns" not in self._compiled_parameters["data_assets"][data_asset][expectation_name]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"] = {}
                                if column_name not in self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name] = {}
                                if param_key not in self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name][param_key] = set()
                                self._compiled_parameters["data_assets"][data_asset][expectation_name]["columns"][column_name][param_key].add(parameter)   
                            
                            elif param_key in ["result", "details"]:
                                if param_key not in self._compiled_parameters["data_assets"][data_asset][expectation_name]:
                                    self._compiled_parameters["data_assets"][data_asset][expectation_name][param_key] = set()
                                self._compiled_parameters["data_assets"][data_asset][expectation_name][param_key].add(parameter)  
                            
                            else:
                                logger.warning("Invalid parameter urn (unrecognized structure): %s" % parameter)

        self._compiled = True

    def review_validation_result(self, url, failed_only=False):
        url = url.strip()
        if url.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
            except ImportError:
                raise ImportError("boto3 is required for retrieving a dataset from s3")
        
            parsed_url = urlparse(url)
            bucket = parsed_url.netloc
            key = parsed_url.path[1:]
            
            s3_response_object = s3.get_object(Bucket=bucket, Key=key)
            object_content = s3_response_object['Body'].read()
            
            results_dict = json.loads(object_content)

            if failed_only:
                failed_results_list = [result for result in results_dict["results"] if not result["success"]]
                results_dict["results"] = failed_results_list
                return results_dict
            else:
                return results_dict
        else:
            raise ValueError("Only s3 urls are supported.")

    def get_failed_dataset(self, validation_result, **kwargs):
        try:
            reference_url = validation_result["meta"]["dataset_reference"]
        except KeyError:
            raise ValueError("Validation result must have a dataset_reference in the meta object to fetch")
        
        if reference_url.startswith("s3://"):
            try:
                import boto3
                s3 = boto3.client('s3')
            except ImportError:
                raise ImportError("boto3 is required for retrieving a dataset from s3")
        
            parsed_url = urlparse(reference_url)
            bucket = parsed_url.netloc
            key = parsed_url.path[1:]
            
            s3_response_object = s3.get_object(Bucket=bucket, Key=key)
            if key.endswith(".csv"):
                # Materialize as dataset
                # TODO: check the associated config for the correct data_asset_type to use
                return read_csv(s3_response_object['Body'], **kwargs)
            else:
                return s3_response_object['Body']

        else:
            raise ValueError("Only s3 urls are supported.")

    def update_return_obj(self, return_obj):
        if self._expectation_explorer:
            return self._expectation_explorer_manager.create_expectation_widget(return_obj)
        else:
            return return_obj
