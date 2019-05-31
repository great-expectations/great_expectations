from itertools import chain
import logging

from IPython.display import display
import ipywidgets as widgets

logger = logging.getLogger(__name__)


class ExpectationExplorer(object):
    def __init__(self):
        self.expectation_widgets = {}
        self.expectation_kwarg_field_names = {
            'expect_column_values_to_be_unique': ['mostly'],
            'expect_column_unique_value_count_to_be_between': ['min_value', 'max_value'],
            'expect_column_value_lengths_to_be_between': ['min_value', 'max_value', 'mostly'],
            'expect_table_row_count_to_be_between': ['min_value', 'max_value'],
            'expect_column_proportion_of_unique_values_to_be_between': ['min_value', 'max_value'],
            'expect_column_median_to_be_between': ['min_value', 'max_value'],
            'expect_column_mean_to_be_between': ['min_value', 'max_value'],
            'expect_column_stdev_to_be_between': ['min_value', 'max_value'],
            'expect_column_sum_to_be_between': ['min_value', 'max_value'],
            'expect_column_values_to_not_be_null': ['mostly'],
            'expect_column_values_to_be_null': ['mostly'],
            'expect_column_values_to_be_json_parseable': ['mostly'],
            'expect_column_values_to_be_dateutil_parseable': ['mostly'],
            'expect_column_values_to_be_increasing': ['strictly', 'parse_strings_as_datetimes', 'mostly'],
            'expect_column_values_to_be_decreasing': ['strictly', 'parse_strings_as_datetimes', 'mostly'],
            'expect_column_values_to_match_regex': ['regex', 'mostly'],
            'expect_column_values_to_not_match_regex': ['regex', 'mostly'],
            'expect_column_values_to_match_json_schema': ['json_schema', 'mostly'],
            'expect_column_values_to_match_regex_list': ['regex_list', 'match_on', 'mostly'],
            'expect_column_values_to_not_match_regex_list': ['regex_list', 'mostly'],
            'expect_column_values_to_be_in_set': ['value_set', 'parse_strings_as_datetimes', 'mostly'],
            'expect_column_values_to_not_be_in_set': ['value_set', 'parse_strings_as_datetimes', 'mostly'],
            'expect_column_most_common_value_to_be_in_set': ['value_set', 'ties_okay'],
            'expect_column_to_exist': ['column_index'],
            'expect_column_value_lengths_to_equal': ['value', 'mostly'],
            'expect_table_row_count_to_equal': ['value'],
            'expect_column_values_to_match_strftime_format': ['strftime_format', 'mostly'],
            ######
            'expect_column_values_to_be_between': ['min_value', 'max_value', 'allow_cross_type_comparisons', 'parse_strings_as_datetimes', 'output_strftime_format', 'mostly'],
            'expect_column_max_to_be_between': ['min_value', 'max_value', 'parse_strings_as_datetimes', 'output_strftime_format'],
            'expect_column_min_to_be_between': ['min_value', 'max_value', 'parse_strings_as_datetimes', 'output_strftime_format'],
            'expect_table_columns_to_match_ordered_list': ['column_list'],
            ####
            'expect_column_pair_values_to_be_equal': ['ignore_row_if'],
            'expect_column_pair_values_A_to_be_greater_than_B': ['or_equal', 'allow_cross_type_comparisons', 'ignore_row_if'],
            'expect_column_pair_values_to_be_in_set': ['value_pairs_set', 'ignore_row_if'],
            'expect_multicolumn_values_to_be_unique': ['ignore_row_if'],
            'expect_column_values_to_be_of_type': ['type_', 'mostly'],
            'expect_column_values_to_be_in_type_list': ['type_list', 'mostly'],
            'expect_column_kl_divergence_to_be_less_than': ['partition_object', 'threshold', 'internal_weight_holdout', 'tail_weight_holdout'],
            'expect_column_chisquare_test_p_value_to_be_greater_than': ['partition_object', 'p'],
            'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than': ['partition_object', 'p', 'bootstrap_samples', 'bootstrap_sample_size'],
            'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than': ['distribution', 'p_value', 'params'],
        }
        self.kwarg_widget_exclusions = [
            'column', 'result_format', 'include_config']
        self.min_max_subtypes = {
            'integer': {
                'positive': [
                    'expect_column_unique_value_count_to_be_between',
                    'expect_column_value_lengths_to_be_between',
                    'expect_table_row_count_to_be_between'
                ],
                'unbounded': ['expect_column_median_to_be_between'],
            },
            'float': {
                'unit_interval': ['expect_column_proportion_of_unique_values_to_be_between'],
                'unbounded': [
                    'expect_column_mean_to_be_between',
                    'expect_column_stdev_to_be_between',
                    'expect_column_sum_to_be_between'
                ]
            }
        }
        self.debug_view = widgets.Output(layout={'border': '3 px solid pink'})

    def update_result(self, *, new_result, column=None):
        new_success_value = new_result.get('success')
        expectation_type = new_result['expectation_config'].get('expectation_type')
        new_result_widgets = self.generate_expectation_result_detail_widgets(result=new_result.get('result', {}))
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

    def set_expectation_state(self, expectation_state, column=None):
        expectation_type = expectation_state.get('expectation_type')
        if column:
            column_expectations = self.expectation_widgets.get(column, {})
            column_expectations[expectation_type] = expectation_state
            self.expectation_widgets[column] = column_expectations
        else:
            non_column_expectations = self.expectation_widgets.get(
                'non_column_expectations', {})
            non_column_expectations[expectation_type] = expectation_state
            self.expectation_widgets['non_column_expectations'] = non_column_expectations

    # interconvert expectation kwargs
    def expectation_kwarg_dict_to_ge_kwargs(self, kwarg_dict):
        expectation_kwargs = {}

        for kwarg_name, widget_dict in kwarg_dict.items():
            if kwarg_name == 'column':
                expectation_kwargs['column'] = widget_dict
                continue
            elif not hasattr(self, f'generate_{kwarg_name}_widget_dict'):
                expectation_kwargs[kwarg_name] = widget_dict.get('ge_kwarg_value')
            else:
                expectation_kwargs[kwarg_name] = widget_dict.get(
                    'ge_kwarg_value') if 'ge_kwarg_value' in widget_dict else widget_dict['kwarg_widget'].value

        return expectation_kwargs

    def update_kwarg_widget_dict(self, *, expectation_state, current_widget_dict, ge_kwarg_name, new_ge_kwarg_value):
        def update_tag_list_widget_dict(widget_dict, new_list):
            widget_dict['ge_kwarg_value'] = new_list
            widget_display = widget_dict['widget_display']
            widget_display.children = self.generate_tag_button_list(
                expectation_state=expectation_state,
                tag_list=new_list,
                widget_display=widget_display
            )
        
        def ge_number_to_widget_string(widget_dict, number_kwarg):
            widget_dict['kwarg_widget'].value = str(number_kwarg)
            widget_dict['ge_kwarg_value'] = number_kwarg

        updater_functions = {
            'regex_list': update_tag_list_widget_dict,
            'value_set': update_tag_list_widget_dict,
            'column_index': ge_number_to_widget_string
        }
        updater_functions.get(
            ge_kwarg_name,
            lambda current_widget_dict, ge_kwarg_value: setattr(current_widget_dict['kwarg_widget'], 'value', ge_kwarg_value)
        )(current_widget_dict, new_ge_kwarg_value)

    def update_expectation_state(self, existing_expectation_state, expectation_validation_result):
        expectation_editor_widget = existing_expectation_state.get(
            'editor_widget')
        new_ge_expectation_kwargs = expectation_validation_result['expectation_config']['kwargs']
        current_expectation_kwarg_dict = existing_expectation_state['kwargs']
        column = current_expectation_kwarg_dict.get('column')

        for ge_kwarg_name, ge_kwarg_value in new_ge_expectation_kwargs.items():
            if ge_kwarg_name in self.kwarg_widget_exclusions:
                continue
            
            current_widget_dict = current_expectation_kwarg_dict.get(ge_kwarg_name)

            if current_widget_dict:
                if not hasattr(self, f'generate_{ge_kwarg_name}_widget_dict'):
                    current_expectation_kwarg_dict[ge_kwarg_name] = self.generate_expectation_kwarg_fallback_widget_dict(
                        expectation_kwarg_name=ge_kwarg_name, **new_ge_expectation_kwargs)
                else:
                    self.update_kwarg_widget_dict(
                        expectation_state=existing_expectation_state,
                        current_widget_dict=current_widget_dict,
                        ge_kwarg_name=ge_kwarg_name,
                        new_ge_kwarg_value=ge_kwarg_value
                    )
            else:
                widget_dict_generator = getattr(
                    self, f'generate_{ge_kwarg_name}_widget_dict', None)
                widget_dict = widget_dict_generator(expectation_state=existing_expectation_state, **new_ge_expectation_kwargs) if widget_dict_generator \
                    else self.generate_expectation_kwarg_fallback_widget_dict(expectation_kwarg_name=ge_kwarg_name, **new_ge_expectation_kwargs)
                current_expectation_kwarg_dict[ge_kwarg_name] = widget_dict
                expectation_editor_widget.children[0].children[1].children += (
                    widget_dict['kwarg_widget'],)

        self.update_result(new_result=expectation_validation_result, column=column)
        return expectation_editor_widget

    # widget generators for general input fields
    def generate_boolean_checkbox_widget(self, *, value, description='', description_tooltip=''):
        return widgets.Checkbox(
            value=value,
            description=description,
            description_tooltip=description_tooltip,
            disabled=False
        )

    def generate_text_area_widget(self, *, value, description='', description_tooltip='', continuous_update=False, placeholder=''):
        return widgets.Textarea(
            value=value,
            placeholder=placeholder, 
            description=description,
            description_tooltip=description_tooltip,
            continuous_update=continuous_update
        )

    def generate_text_widget(self, *, value, description='', description_tooltip='', continuous_update=False, placeholder=''):
        return widgets.Text(
            value=value,
            placeholder=placeholder,
            description=description,
            description_tooltip=description_tooltip,
            continuous_update=continuous_update
        )

    def generate_radio_buttons_widget(self, *, options=[], value, description='', description_tooltip=''):
        return widgets.RadioButtons(
            options=options,
            value=value,
            description=description,
            description_tooltip=description_tooltip
        )

    def generate_tag_button(self, *, expectation_state, tag, tag_list, widget_display):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        tag_button = widgets.Button(
            description=str(tag),
            button_style='danger',
            tooltip='click to delete',
            icon='trash',
            layout={'width': 'auto'}
        )

        @exception_widget.capture(clear_output=True)
        def on_click(button):
            if len(tag_list) == 1:
                with exception_widget:
                    print('Warning: cannot remove - set must have at least one member')
                return
            tag_list.remove(tag)

            display_children = list(widget_display.children)
            display_children.remove(tag_button)
            widget_display.children = display_children

            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        tag_button.on_click(on_click)

        return tag_button

    def generate_tag_button_list(self, *, expectation_state, tag_list, widget_display):
        return [
            self.generate_tag_button(
                expectation_state=expectation_state,
                tag=tag,
                tag_list=tag_list,
                widget_display=widget_display
            ) for tag in tag_list
        ]

    def generate_zero_or_positive_integer_widget(self, *, value, max=int(9e300), description='', continuous_update=False):
        return widgets.BoundedIntText(
            value=value,
            min=0,
            max=max,
            description=description,
            continuous_update=continuous_update
        )

    # widget dict generators for kwarg input fields
    def generate_output_strftime_format_widget_dict(self, *, expectation_state, output_strftime_format='', column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']

        output_strftime_format_widget = self.generate_text_widget(
            value=output_strftime_format,
            description='output strftime format: ',
            placeholder='press enter to confirm...',
            description_tooltip='Enter a valid strfime format for datetime output.'
        )

        widget_dict = {
            'kwarg_widget': output_strftime_format_widget
        }

        @exception_widget.capture(clear_output=True)
        def on_output_strftime_format_submit(widget):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        output_strftime_format_widget.on_submit(on_output_strftime_format_submit)

        return widget_dict

    def generate_strftime_format_widget_dict(self, *, expectation_state, strftime_format='', column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']

        strftime_format_widget = self.generate_text_widget(
            value=strftime_format,
            description='strftime format: ',
            placeholder='press enter to confirm...'
        )

        widget_dict = {
            'kwarg_widget': strftime_format_widget
        }

        @exception_widget.capture(clear_output=True)
        def on_strftime_format_submit(widget):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        strftime_format_widget.on_submit(on_strftime_format_submit)

        return widget_dict

    def generate_value_widget_dict(self, *, expectation_state, value=None, column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']

        value_widget = self.generate_zero_or_positive_integer_widget(
            value=value, description='value: ')

        @exception_widget.capture(clear_output=True)
        def on_value_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        value_widget.observe(on_value_change, names='value')

        widget_dict = {
            'kwarg_widget': value_widget
        }

        return widget_dict

    def generate_json_schema_widget_dict(self, *, expectation_state, json_schema='', column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']

        json_schema_widget = self.generate_text_area_widget(value=json_schema, description='json schema', placeholder='enter json schema')

        @exception_widget.capture(clear_output=True)
        def on_json_schema_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        json_schema_widget.observe(
            on_json_schema_change, names='value')

        widget_dict = {
            'kwarg_widget': json_schema_widget
        }

        return widget_dict

    def generate_ties_okay_widget_dict(self, *, expectation_state, ties_okay=None, column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        ties_okay_widget = self.generate_boolean_checkbox_widget(
            value=ties_okay,
            description='ties_okay: ',
            description_tooltip='If True, then the expectation will still succeed if values outside the designated set are as common (but not more common) than designated values.'
        )

        @exception_widget.capture(clear_output=True)
        def on_ties_okay_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        ties_okay_widget.observe(
            on_ties_okay_change, names='value')

        widget_dict = {
            'kwarg_widget': ties_okay_widget
        }

        return widget_dict

    def generate_match_on_widget_dict(self, *, expectation_state, match_on='any', column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        match_on_widget = self.generate_radio_buttons_widget(
            options=['any', 'all'],
            value=match_on,
            description='match_on: ',
            description_tooltip='Use “any” if the value should match at least one regular expression in the list. Use “all” if it should match each regular expression in the list.'
        )

        @exception_widget.capture(clear_output=True)
        def on_match_on_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        match_on_widget.observe(
            on_match_on_change, names='value')

        widget_dict = {
            'kwarg_widget': match_on_widget
        }

        return widget_dict

    def generate_regex_list_widget_dict(self, *, expectation_state, regex_list=[], column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        regex_list_widget_display = widgets.Box()
        regex_list_widget_input = self.generate_text_widget(value=None, description='add regex: ', placeholder='press enter to add...', continuous_update=True)
        regex_list_label = widgets.HTML(value='<strong>regex list: </strong>')
        regex_list_widget = widgets.VBox([regex_list_label, regex_list_widget_display, regex_list_widget_input])

        widget_dict = {
            'kwarg_widget': regex_list_widget,
            'ge_kwarg_value': regex_list,
            'widget_display': regex_list_widget_display,
            'widget_input': regex_list_widget_input
        }

        regex_list_widget_display.children = self.generate_tag_button_list(
            expectation_state=expectation_state,
            tag_list=regex_list,
            widget_display=regex_list_widget_display
        )

        @exception_widget.capture(clear_output=True)
        def on_regex_list_input_submit(widget):
            widget_display = expectation_state['kwargs']['regex_list']['widget_display']
            regex_list = expectation_state['kwargs']['regex_list']['ge_kwarg_value']
            new_regex = widget.value

            if new_regex in regex_list:
                with exception_widget:
                    print('Warning: regex already exists in set')
                return

            regex_list.append(new_regex)
            widget_display.children += (self.generate_tag_button(
                tag=new_regex,
                expectation_state=expectation_state,
                tag_list=regex_list,
                widget_display=widget_display), 
            )
            widget.value = ''

            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)
        regex_list_widget_input.on_submit(on_regex_list_input_submit)

        return widget_dict

    def generate_column_index_widget_dict(self, *, expectation_state, column_index='', column, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        
        column_index_widget = self.generate_text_widget(value=str(column_index), description='column_index: ', continuous_update=True, placeholder='press enter to confirm...')

        widget_dict = {
            'kwarg_widget': column_index_widget,
            'ge_kwarg_value': column_index
        }

        @exception_widget.capture(clear_output=True)
        def on_column_index_submit(widget):
            new_column_index = widget.value
            if '.' in new_column_index or '-' in new_column_index:
                with exception_widget:
                    print('Warning: column_index must be an integer >= 0')
                return
            try:
                new_column_index = int(new_column_index)
            except ValueError:
                with exception_widget:
                    print('Warning: column_index must be an integer >= 0')
                return

            widget_dict['ge_kwarg_value'] = new_column_index

            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        column_index_widget.on_submit(on_column_index_submit)

        return widget_dict

    def generate_regex_widget_dict(self, *, expectation_state, regex='', column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        regex_widget = self.generate_text_area_widget(
            value=regex, description='regex: ', placeholder='e.g. ([A-Z])\w+')

        @exception_widget.capture(clear_output=True)
        def on_regex_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        regex_widget.observe(
            on_regex_change, names='value')
        
        widget_dict = {
            'kwarg_widget': regex_widget
        }

        return widget_dict

    def generate_parse_strings_as_datetimes_widget_dict(
            self, *, expectation_state, parse_strings_as_datetimes=None, column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        parse_strings_as_datetimes_widget = self.generate_boolean_checkbox_widget(
            value=parse_strings_as_datetimes,
            description='parse strings as datetimes'
        )

        @exception_widget.capture(clear_output=True)
        def on_parse_strings_as_datetimes_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        parse_strings_as_datetimes_widget.observe(
            on_parse_strings_as_datetimes_change, names='value')

        widget_dict = {
            'kwarg_widget': parse_strings_as_datetimes_widget
        }

        return widget_dict
 
    def generate_strictly_widget_dict(self, *, expectation_state, strictly=None, column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        inc_dec = expectation_type.split('_')[-1]
        strictly_widget = self.generate_boolean_checkbox_widget(
            value=strictly,
            description=f'strictly {inc_dec}'
        )

        @exception_widget.capture(clear_output=True)
        def on_strictly_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        strictly_widget.observe(on_strictly_change, names='value')

        widget_dict = {
            'kwarg_widget': strictly_widget
        }

        return widget_dict

    def generate_mostly_widget_dict(self, *, mostly=1, expectation_state, column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
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

        @exception_widget.capture(clear_output=True)
        def on_mostly_change(change):
            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        mostly_widget.observe(on_mostly_change, names='value')
        widget_dict = {
            'kwarg_widget': mostly_widget
        }

        return widget_dict
    
    def generate_min_value_widget_dict(self, *, expectation_state, min_value=None, column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        min_value_widget_dict = expectation_state['kwargs'].get('min_value')
        min_value_widget = min_value_widget_dict.get('kwarg_widget') if min_value_widget_dict else None
        max_value_widget_dict = expectation_state['kwargs'].get('max_value')
        max_value_widget = max_value_widget_dict.get('kwarg_widget') if max_value_widget_dict else None

        # integer
        if expectation_type in list(chain.from_iterable(self.min_max_subtypes['integer'].values())):
            if expectation_type in self.min_max_subtypes['integer']['positive']:
                default_min_value = 0
                default_max_value = int(9e300)
            elif expectation_type in self.min_max_subtypes['integer']['unbounded']:
                default_min_value = -int(9e300)
                default_max_value = int(9e300)

            if min_value_widget:
                min_value_widget.value = min_value or default_min_value
            else:
                min_value_widget = widgets.BoundedIntText(
                    value=min_value or default_min_value,
                    min=default_min_value,
                    description='min_value: ',
                    disabled=False
                )
            if not max_value_widget:
                max_value_widget = widgets.BoundedIntText(
                    description='max_value: ',
                    value=default_max_value,
                    max=default_max_value,
                    disabled=False
                )
        # float
        elif expectation_type in list(chain.from_iterable(self.min_max_subtypes['float'].values())):
            if expectation_type in self.min_max_subtypes['float']['unit_interval']:
                default_min_value = 0
                default_max_value = 1
            elif expectation_type in self.min_max_subtypes['float']['unbounded']:
                default_min_value = -int(9e300)
                default_max_value = int(9e300)

            if min_value_widget:
                min_value_widget.value = min_value or default_min_value
            else:
                min_value_widget = widgets.BoundedFloatText(
                    value=min_value or default_min_value,
                    min=default_min_value,
                    step=0.01,
                    description='min_value: ',
                    disabled=False
                )
            if not max_value_widget:
                max_value_widget = widgets.BoundedFloatText(
                    description='max_value: ',
                    value=default_max_value,
                    max=default_max_value,
                    step=0.01,
                    disabled=False
                )

        if min_value_widget and max_value_widget:
            @exception_widget.capture(clear_output=True)
            def on_min_value_change(change):
                ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                    expectation_state['kwargs'])
                getattr(ge_df, expectation_type)(include_config=True,
                    **ge_expectation_kwargs)

            min_value_widget.observe(on_min_value_change, names='value')
            max_dl = widgets.link((max_value_widget, 'value'),
                                (min_value_widget, 'max'))
            expectation_state['kwargs']['max_value'] = {'kwarg_widget': max_value_widget}

        min_value_widget_dict = {'kwarg_widget': min_value_widget} or self.generate_expectation_kwarg_fallback_widget_dict(
            expectation_kwarg_name='min_value', **{'min_value': min_value})
        expectation_state['kwargs']['min_value'] = min_value_widget_dict

        self.set_expectation_state(expectation_state, column)

        return min_value_widget_dict

    def generate_max_value_widget_dict(self, *, expectation_state, max_value=None, column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']
        min_value_widget_dict = expectation_state['kwargs'].get('min_value')
        min_value_widget = min_value_widget_dict.get('kwarg_widget') if min_value_widget_dict else None
        max_value_widget_dict = expectation_state['kwargs'].get('max_value')
        max_value_widget = max_value_widget_dict.get('kwarg_widget') if max_value_widget_dict else None

        # integer
        if expectation_type in list(chain.from_iterable(self.min_max_subtypes['integer'].values())):
            if expectation_type in self.min_max_subtypes['integer']['positive']:
                default_min_value = 0
                default_max_value = int(9e300)
            elif expectation_type in self.min_max_subtypes['integer']['unbounded']:
                default_min_value = -int(9e300)
                default_max_value = int(9e300)

            if max_value_widget:
                max_value_widget.value = max_value or default_max_value
            else:
                max_value_widget = widgets.BoundedIntText(
                    value=max_value or default_max_value,
                    max=default_max_value,
                    description='max_value: ',
                    disabled=False
                )
            if not min_value_widget:
                min_value_widget = widgets.BoundedIntText(
                    min=default_min_value,
                    value=default_min_value,
                    description='min_value: ',
                    disabled=False
                )
        # float
        elif expectation_type in list(chain.from_iterable(self.min_max_subtypes['float'].values())):
            if expectation_type in self.min_max_subtypes['float']['unit_interval']:
                default_min_value = 0
                default_max_value = 1
            elif expectation_type in self.min_max_subtypes['float']['unbounded']:
                default_min_value = -int(9e300)
                default_max_value = int(9e300)
            
            if max_value_widget:
                max_value_widget.value = max_value or default_max_value
            else:
                max_value_widget = widgets.BoundedFloatText(
                    value=max_value or default_max_value,
                    max=default_max_value,
                    step=0.01,
                    description='max_value: ',
                    disabled=False
                )
            if not min_value_widget:
                min_value_widget = widgets.BoundedFloatText(
                    min=default_min_value,
                    step=0.01,
                    value=default_min_value,
                    description='min_value: ',
                    disabled=False
                )

        if min_value_widget and max_value_widget:
            @exception_widget.capture(clear_output=True)
            def on_max_value_change(change):
                ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                    expectation_state['kwargs'])
                getattr(ge_df, expectation_type)(include_config=True,
                    **ge_expectation_kwargs)

            max_value_widget.observe(on_max_value_change, names='value')
            min_dl = widgets.link((min_value_widget, 'value'),
                                (max_value_widget, 'min'))
            expectation_state['kwargs']['min_value'] = {'kwarg_widget': min_value_widget}

        max_value_widget_dict = {'kwarg_widget': max_value_widget} or self.generate_expectation_kwarg_fallback_widget_dict(
            expectation_kwarg_name='max_value', **{'max_value': max_value}
        )
        expectation_state['kwargs']['max_value'] = max_value_widget_dict

        self.set_expectation_state(expectation_state, column)

        return max_value_widget_dict

    def generate_value_set_widget_dict(self, *, expectation_state, value_set=[], column=None, **expectation_kwargs):
        ge_df = expectation_state['ge_df']
        exception_widget = expectation_state['exception_widget']
        expectation_type = expectation_state['expectation_type']

        value_set_widget_display = widgets.Box()

        value_set_widget_number_input = self.generate_text_widget(
            value=None, description='add number: ', placeholder='press enter to add number...', continuous_update=True)
        value_set_widget_string_input = self.generate_text_widget(
            value=None, description='add string: ', placeholder='press enter to add string...', continuous_update=True)
        value_set_widget_input = widgets.VBox([value_set_widget_string_input, value_set_widget_number_input])
        value_set_label = widgets.HTML(value='<strong>value set: </strong>')
        value_set_widget = widgets.VBox([value_set_label, value_set_widget_display, value_set_widget_input])

        widget_dict = {
            'kwarg_widget': value_set_widget,
            'ge_kwarg_value': value_set,
            'widget_display': value_set_widget_display,
            'widget_input': value_set_widget_input
        }

        value_set_widget_display.children = self.generate_tag_button_list(
            expectation_state=expectation_state,
            tag_list=value_set,
            widget_display=value_set_widget_display
        )

        @exception_widget.capture(clear_output=True)
        def on_value_set_number_input_submit(widget):
            widget_display = expectation_state['kwargs']['value_set']['widget_display']
            value_set = expectation_state['kwargs']['value_set']['ge_kwarg_value']
            new_value = widget.value

            new_value = float(new_value) if '.' in new_value else int(new_value)

            if new_value in value_set:
                with exception_widget:
                    print('Warning: value already exists in set')
                return

            value_set.append(new_value)
            widget_display.children += (
                self.generate_tag_button(
                    tag=new_value,
                    expectation_state=expectation_state,
                    tag_list=value_set,
                    widget_display=widget_display
                ),
            )
            widget.value = ''

            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(
                include_config=True, **ge_expectation_kwargs)

        @exception_widget.capture(clear_output=True)
        def on_value_set_string_input_submit(widget):
            widget_display = expectation_state['kwargs']['value_set']['widget_display']
            value_set = expectation_state['kwargs']['value_set']['ge_kwarg_value']
            new_value = widget.value

            if new_value in value_set:
                with exception_widget:
                    print('Warning: value already exists in set')
                return

            value_set.append(new_value)
            widget_display.children += (
                self.generate_tag_button(
                    tag=new_value,
                    expectation_state=expectation_state,
                    tag_list=value_set,
                    widget_display=widget_display
                ),
            )
            widget.value = ''

            ge_expectation_kwargs = self.expectation_kwarg_dict_to_ge_kwargs(
                expectation_state['kwargs'])

            getattr(ge_df, expectation_type)(include_config=True, **ge_expectation_kwargs)

        value_set_widget_number_input.on_submit(on_value_set_number_input_submit)
        value_set_widget_string_input.on_submit(on_value_set_string_input_submit)

        return widget_dict

    def generate_expectation_kwarg_fallback_widget_dict(self, *, expectation_kwarg_name, **expectation_kwargs):
        ge_kwarg_value = expectation_kwargs.get(
            expectation_kwarg_name)
        warning_message = widgets.HTML(
            value=f'<div><strong>Warning: </strong>No input widget for kwarg "{expectation_kwarg_name}". To change value, call expectation again with the modified value.</div>'
        )
        static_widget = widgets.Textarea(value=str(
            ge_kwarg_value), description=f'{expectation_kwarg_name}: ', disabled=True)
        widget_dict = {
            'kwarg_widget': widgets.VBox([warning_message, static_widget]),
            'ge_kwarg_value': ge_kwarg_value
        }
        
        return widget_dict

    # widget generators for general info shared between all expectations
    def generate_column_widget(self, column=None):
        #         return widgets.HTML(value=f'<span><strong>Column: </strong>{column}</span>') if column else None
        return widgets.HTML(value=f'<div><strong>Column: </strong>{column}</div>') if column else None

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

    def generate_expectation_result_detail_widgets(self, *, result={}):
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
            return self.update_expectation_state(existing_expectation_state, expectation_validation_result)

        # success_widget
        success = expectation_validation_result['success']
        success_widget = widgets.HTML(
            value='<span><strong>Success: </strong>{str(success)}</span>'.format(success=success))

        # widget with basic info (column, expectation_type)
        basic_expectation_info_box = self.generate_basic_expectation_info_box(
            expectation_type, success_widget, column)

        # widget with result details
        result_detail_widget = widgets.VBox(children=self.generate_expectation_result_detail_widgets(
            result=expectation_validation_result.get("result", {})
        ))

        # accordion container for result_detail_widget
        result_detail_accordion = widgets.Accordion(
            children=[result_detail_widget])
        result_detail_accordion.set_title(0, 'Validation Result Details')
        result_detail_accordion.selected_index = None

        # widget for displaying kwarg widget exceptions
        exception_widget = widgets.Output(layout={'border': '3 px solid pink'})
        exception_accordion = widgets.Accordion(
            children=[exception_widget]
        )
        exception_accordion.set_title(0, 'Exceptions/Warnings')

        # create scaffold dict for expectation state
        expectation_state = {
            'success': success_widget,
            'expectation_type': expectation_type,
            'exception_widget': exception_widget,
            'ge_df': ge_df,
            'result_detail_widget': result_detail_widget,
            'kwargs': {
            }
        }
        if column:
            expectation_state['kwargs']['column'] = column

        # collect widget dicts for kwarg inputs
        expectation_kwarg_input_widget_dicts = []
        for expectation_kwarg_name in self.expectation_kwarg_field_names.get(expectation_type):
            widget_dict_generator = getattr(
                self, f'generate_{expectation_kwarg_name}_widget_dict', None)
            widget_dict = widget_dict_generator(
                expectation_state=expectation_state,
                **expectation_kwargs) if widget_dict_generator else \
                    self.generate_expectation_kwarg_fallback_widget_dict(expectation_kwarg_name=expectation_kwarg_name, **expectation_kwargs)
            expectation_state['kwargs'][expectation_kwarg_name] = widget_dict
            expectation_kwarg_input_widget_dicts.append(widget_dict)

        # container for kwarg input widgets
        expectation_kwarg_input_box = widgets.VBox(
            children=[dict['kwarg_widget'] for dict in expectation_kwarg_input_widget_dicts],
            layout=widgets.Layout(margin='10px')
        )

        # top-level widget container
        expectation_editor_widget = widgets.VBox(
            [
                widgets.HBox([basic_expectation_info_box,
                              expectation_kwarg_input_box]),
                exception_accordion,
                result_detail_accordion,
            ],
            layout=widgets.Layout(
                border='2px solid {color}'.format(color="green" if success else "red"),
                margin='5px'
            )
        )

        # complete expectation state scaffold
        expectation_state['editor_widget'] = expectation_editor_widget

        # set expectation state
        self.set_expectation_state(expectation_state, column)

        return expectation_editor_widget

    def edit_expectations(self):
        expectation_widget_list = []
        for expectation_collection in self.expectation_widgets.values():
            for expectation in expectation_collection.values():
                expectation_widget_list.append(
                    expectation.get('editor_widget'))
        for widget in expectation_widget_list:
            display(widget)
