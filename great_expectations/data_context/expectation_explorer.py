import logging

from IPython.display import display
import ipywidgets as widgets

logger = logging.getLogger(__name__)
debug_view = widgets.Output(layout={'border': '3 px solid pink'})


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
            'expect_column_values_to_be_in_type_list': ['type_list', 'mostly'],
            'expect_multicolumn_values_to_be_unique': ['ignore_row_if'],
            'expect_column_chisquare_test_p_value_to_be_greater_than': ['partition_object', 'p'],
            'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than': ['partition_object', 'p', 'bootstrap_samples', 'bootstrap_sample_size'],
            'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than': ['distribution', 'p_value', 'params'],
        }
        self.kwarg_widget_exclusions = [
            'column', 'result_format', 'include_config']
        # debug_view is for debugging ipywidgets callback functions
        self.debug_view = widgets.Output(layout={'border': '3 px solid pink'})

    # @debug_view.capture(clear_output=True)
    # def update_result(self, *, new_result, expectation_type, column=None):
    def update_result(self, new_result, expectation_type, column=None):
        new_success_value = new_result.get('success')
        new_result_widgets = self.generate_expectation_result_detail_widgets(
            new_result['result'], expectation_type)
        new_border_color = 'green' if new_success_value else 'red'

        if column:
            self.expectation_widgets[column][expectation_type][
                'success'].value = '<span><strong>Success: </strong>{str(new_success_value)}</span>'.format(new_success_value=new_success_value)
            self.expectation_widgets[column][expectation_type]['result_detail_widget']\
                .children = new_result_widgets
            self.expectation_widgets[column][expectation_type]['editor_widget']\
                .layout\
                .border = '2px solid {new_border_color}'.format(new_border_color=new_border_color)
        else:
            self.expectation_widgets['non_column_expectations'][expectation_type][
                'success'].value = '<span><strong>Success: </strong>{str(new_success_value)}</span>'.format(new_success_value=new_success_value)
            self.expectation_widgets['non_column_expectations'][expectation_type]['result_detail_widget']\
                .children = new_result_widgets
            self.expectation_widgets['non_column_expectations'][expectation_type]['editor_widget']\
                .layout\
                .border = '2px solid {new_border_color}'.format(new_border_color=new_border_color)

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

    @debug_view.capture(clear_output=True)
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
            if kwarg_name in self.kwarg_widget_exclusions:
                continue
            existing_widget = existing_expectation_kwarg_widgets.get(
                kwarg_name)

            if existing_widget:
                if getattr(existing_widget, 'value', None) or getattr(
                        existing_widget, 'value', None) == 0:
                    existing_widget.value = kwarg_value
                else:
                    existing_expectation_kwarg_widgets[kwarg_name] = kwarg_value
            else:
                widget_generator = getattr(
                    self, 'generate_{kwarg_name}_widget'.format(kwarg_name=kwarg_name), None)
                widget = widget_generator(ge_df=ge_df, expectation_type=expectation_type, **new_expectation_kwargs) if widget_generator \
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
            kwarg_value = kwarg_value.value if (getattr(
                kwarg_value, 'value', None) or getattr(
                kwarg_value, 'value', None) == 0) else kwarg_value
            transformers = {
                'value_set': lambda value_set_string: [item.strip() for item in value_set_string.split(',')]
            }
            return transformers.get(kwarg_key, lambda kwarg_value: kwarg_value)(kwarg_value)

        expectation_kwargs = {}

        for key, value in kwarg_widgets.items():
            if not getattr(self, 'generate_{key}_widget'.format(key=key), None):
                continue
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
            if not getattr(self, 'generate_{key}_widget'.format(key=key), None):
                continue
            expectation_kwargs[key] = kwarg_transformer(key, value)

        return expectation_kwargs

    # widget generators for input fields
    # def generate_mostly_widget(self, *, ge_df, mostly=1, expectation_type, column=None, **expectation_kwargs):
    def generate_mostly_widget(self, ge_df, mostly, expectation_type, column=None, **expectation_kwargs):
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

        @debug_view.capture(clear_output=True)
        def on_mostly_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])

            new_result = getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)
            # self.update_result(new_result=new_result, expectation_type=expectation_type, column=column)

        mostly_widget.observe(on_mostly_change, names='value')
        return mostly_widget

    # def generate_min_value_widget(self, *, ge_df, expectation_type, min_value=None, column=None, **expectation_kwargs):
    def generate_min_value_widget(self, ge_df, expectation_type, min_value=None, column=None, **expectation_kwargs):

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

        @debug_view.capture(clear_output=True)
        def on_min_value_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])
            new_result = getattr(ge_df, expectation_type)(include_config=True,
                **ge_expectation_kwargs)

        min_value_widget.observe(on_min_value_change, names='value')
        max_dl = widgets.link((max_value_widget, 'value'),
                              (min_value_widget, 'max'))

        expectation_state['kwargs']['min_value'] = min_value_widget
        expectation_state['kwargs']['max_value'] = max_value_widget
        self.set_expectation_state(expectation_type, expectation_state, column)

        return min_value_widget

    # def generate_max_value_widget(self, *, ge_df, expectation_type, max_value=None, column=None, **expectation_kwargs):
    def generate_max_value_widget(self, ge_df, expectation_type, max_value=None, column=None, **expectation_kwargs):
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

        @debug_view.capture(clear_output=True)
        def on_max_value_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])
            new_result = getattr(ge_df, expectation_type)(include_config=True,
                **ge_expectation_kwargs)

        max_value_widget.observe(on_max_value_change, names='value')
        min_dl = widgets.link((min_value_widget, 'value'),
                              (max_value_widget, 'min'))

        expectation_state['kwargs']['min_value'] = min_value_widget
        expectation_state['kwargs']['max_value'] = max_value_widget
        self.set_expectation_state(expectation_type, expectation_state, column)

        return max_value_widget

#     def generate_value_set_widget(self, *, ge_df, expectation_type, value_set, column, **expectation_kwargs):
    def generate_value_set_widget(self, ge_df, expectation_type, value_set, column, **expectation_kwargs):
        expectation_state = self.get_expectation_state(
            expectation_type, column)
        value_set_widget = widgets.Textarea(
            value=', '.join(value_set),
            placeholder='Please enter comma-separated set values.',
            description='value_set: ',
            disabled=False
        )

        @debug_view.capture(clear_output=True)
        def on_value_set_change(change):
            expectation_state = self.get_expectation_state(
                expectation_type, column)
            ge_expectation_kwargs = self.expectation_kwarg_widgets_to_ge_kwargs(
                expectation_state['kwargs'])

            new_result = getattr(ge_df, expectation_type)(include_config=True,
                                                          **ge_expectation_kwargs)

            self.update_result(new_result=new_result,
                               expectation_type=expectation_type, column=column)

        value_set_widget.observe(on_value_set_change, names='value')
        return value_set_widget

#    def generate_expectation_kwarg_fallback_widget(self, *, expectation_kwarg_name, **expectation_kwargs):
    def generate_expectation_kwarg_fallback_widget(self, expectation_kwarg_name, **expectation_kwargs):
        expectation_kwarg_value = expectation_kwargs.get(
            expectation_kwarg_name)
        warning_message = widgets.HTML(
            value='<div><strong>Warning: </strong>Cannot find dynamic widget for expectation kwarg "{expectation_kwarg_name}". To change kwarg value, please call expectation again with the modified value.</div>'.format(expectation_kwarg_name=expectation_kwarg_name)
        )
        static_widget = widgets.Textarea(value=str(
            expectation_kwarg_value), description='{expectation_kwarg_name}: '.format(expectation_kwarg_name=expectation_kwarg_name), disabled=True)
        return widgets.VBox([warning_message, static_widget])

    # widget generators for general info shared between all expectations
    def generate_column_widget(self, column, **kwargs):
        #         return widgets.HTML(value=f'<span><strong>Column: </strong>{column}</span>') if column else None
        return widgets.HTML(value='<div><style type<strong>Column: </strong>{column}</div>'.format(column=column)) if column else None

    def generate_expectation_type_widget(self, expectation_type):
        return widgets.HTML(value='<span><strong>Expectation Type: </strong>{expectation_type}</span>'.format(expectation_type=expectation_type))

    def generate_success_widget(self, success):
        return widgets.HTML(value='<span><strong>Success: </strong>{str(success)}</span>'.format(success=success))

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
            value='<span><strong>Success: </strong>{str(success)}</span>'.format(success=success))

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
                self, 'generate_{expectation_kwarg_name}_widget'.format(expectation_kwarg_name=expectation_kwarg_name), None)
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
                border='2px solid {color}'.format(color="green" if success else "red"),
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
