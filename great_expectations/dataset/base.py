from .util import DotDict, recursively_convert_to_json_serializable

import json
import inspect
import copy
from functools import wraps
import traceback
import warnings

import pandas as pd
from collections import defaultdict

from ..version import __version__

class DataSet(object):

    def __init__(self, *args, **kwargs):
        super(DataSet, self).__init__(*args, **kwargs)
        self.initialize_expectations()

    @classmethod
    def expectation(cls, method_arg_names):
        """
        The core expectation decorator, this method takes a single parameter which it uses to build and save the
        expectation config to the DataSet object. The parameter defines an ordered list of the positional arguments to
        be used by the method implementing the expectation.

        Note that intermediate decorators that call the core @expectation decorator will most likely need to pass their
        decorated methods' signature up to the expectation decorator. For example, the MetaPandasDataSet column_map_expectation
        decorator relies on the DataSet expectation decorator, but will pass through the signature from the implementing method.

        When decorated with @expectation, a method will:
            1. Build and update the expectation config.
            2. Handle the "include_config" boolean parameter, which allows a caller to retrieve the generated
                configuration immediately after running the expectation.
            3. Handle the "catch_exceptions" parameter, which allows a caller to catch any exception and report an
                aggregate trace, useful for validation.
            4. Handle the "output_format" parameter, and pass it down to the implementing method if its signature expects it.
                By handing down the output_format, methods implementing expectations can optionally provide additional output formats
                specific to the use cases that they handle.
        """
        def outer_wrapper(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):

                #Get the name of the method
                method_name = func.__name__

                # Combine all arguments into a single new "kwargs"
                all_args = dict(zip(method_arg_names, args))
                all_args.update(kwargs)

                #Unpack display parameters; remove them from all_args if appropriate
                if "include_config" in kwargs:
                    include_config = kwargs["include_config"]
                    del all_args["include_config"]
                else:
                    include_config = self.default_expectation_args["include_config"]

                if "catch_exceptions" in kwargs:
                    catch_exceptions = kwargs["catch_exceptions"]
                    del all_args["catch_exceptions"]
                else:
                    catch_exceptions = self.default_expectation_args["catch_exceptions"]

                if "output_format" in kwargs:
                    output_format = kwargs["output_format"]
                else:
                    output_format = self.default_expectation_args["output_format"]

                if "meta" in kwargs:
                    meta = kwargs["meta"]
                    del all_args["meta"]
                else:
                    meta = None

                if "meta_notes" in kwargs:
                    meta = { "notes": kwargs["meta_notes"] }
                    del all_args["meta_notes"]

                # This intends to get the signature of the inner wrapper, if there is one.
                if "output_format" in inspect.getargspec(func)[0][1:]:
                    all_args["output_format"] = output_format
                else:
                    if "output_format" in all_args:
                        del all_args["output_format"]

                all_args = recursively_convert_to_json_serializable(all_args)
                expectation_args = copy.deepcopy(all_args)

                #Construct the expectation_config object
                expectation_config = DotDict({
                    "expectation_type": method_name,
                    "kwargs": expectation_args
                })

                if meta is not None:
                    expectation_config["meta"] = meta

                raised_exception = False
                exception_traceback = None

                #Finally, execute the expectation method itself
                try:
                    return_obj = func(self, **expectation_args)

                except Exception as err:
                    if catch_exceptions:
                        raised_exception = True
                        exception_traceback = traceback.format_exc()

                        if output_format != "BOOLEAN_ONLY":
                            return_obj = {
                                "success": False
                            }
                        else:
                            return_obj = False
                    else:
                        raise(err)

                #Add a "success" object to the config
                if output_format == "BOOLEAN_ONLY":
                    expectation_config["success_on_last_run"] = return_obj
                else:
                    expectation_config["success_on_last_run"] = return_obj["success"]

                #Append the expectation to the config.
                self.append_expectation(expectation_config)

                if output_format != 'BOOLEAN_ONLY':

                    if include_config:
                        return_obj["expectation_type"] = expectation_config["expectation_type"]
                        return_obj["expectation_kwargs"] = copy.deepcopy(dict(expectation_config["kwargs"]))

                    if catch_exceptions:
                        return_obj["raised_exception"] = raised_exception
                        return_obj["exception_traceback"] = exception_traceback

                return return_obj

            # wrapper.__name__ = func.__name__
            # wrapper.__doc__ = func.__doc__
            return wrapper

        return outer_wrapper

    @classmethod
    def column_map_expectation(cls, func):
        raise NotImplementedError

    @classmethod
    def column_aggregate_expectation(cls, func):
        raise NotImplementedError

    def initialize_expectations(self, config=None, name=None):
        if config != None:
            #!!! Should validate the incoming config with jsonschema here

            # Copy the original so that we don't overwrite it by accident
            self._expectations_config = DotDict(copy.deepcopy(config))

        else:
            self._expectations_config = DotDict({
                "dataset_name" : name,
                "meta": {
                    "great_expectations.__version__": __version__
                },
                "expectations" : []
            })

        self.default_expectation_args = {
            "include_config" : False,
            "catch_exceptions" : False,
            "output_format" : 'BASIC',
        }

    def append_expectation(self, expectation_config):
        expectation_type = expectation_config['expectation_type']

        #Test to ensure the new expectation is serializable.
        #FIXME: If it's not, are we sure we want to raise an error?
        #FIXME: Should we allow users to override the error?
        #FIXME: Should we try to convert the object using something like recursively_convert_to_json_serializable?
        json.dumps(expectation_config)

        #Drop existing expectations with the same expectation_type.
        #For column_expectations, append_expectation should only replace expectations
        # where the expectation_type AND the column match
        #!!! This is good default behavior, but
        #!!!    it needs to be documented, and
        #!!!    we need to provide syntax to override it.

        if 'column' in expectation_config['kwargs']:
            column = expectation_config['kwargs']['column']

            self._expectations_config.expectations = [f for f in filter(
                lambda exp: (exp['expectation_type'] != expectation_type) or ('column' in exp['kwargs'] and exp['kwargs']['column'] != column),
                self._expectations_config.expectations
            )]
        else:
            self._expectations_config.expectations = [f for f in filter(
                lambda exp: exp['expectation_type'] != expectation_type,
                self._expectations_config.expectations
            )]

        self._expectations_config.expectations.append(expectation_config)

    def _copy_and_clean_up_expectation(self,
        expectation,
        discard_output_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        new_expectation = copy.deepcopy(expectation)

        if "success_on_last_run" in new_expectation:
            del new_expectation["success_on_last_run"]

        if discard_output_format_kwargs:
            if "output_format" in new_expectation["kwargs"]:
                del new_expectation["kwargs"]["output_format"]
                # discards["output_format"] += 1

        if discard_include_configs_kwargs:
            if "include_configs" in new_expectation["kwargs"]:
                del new_expectation["kwargs"]["include_configs"]
                # discards["include_configs"] += 1

        if discard_catch_exceptions_kwargs:
            if "catch_exceptions" in new_expectation["kwargs"]:
                del new_expectation["kwargs"]["catch_exceptions"]
                # discards["catch_exceptions"] += 1

        return new_expectation

    def _copy_and_clean_up_expectations_from_indexes(
        self,
        match_indexes,
        discard_output_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        rval = []
        for i in match_indexes:
            rval.append(
                self._copy_and_clean_up_expectation(
                    self._expectations_config.expectations[i],
                    discard_output_format_kwargs,
                    discard_include_configs_kwargs,
                    discard_catch_exceptions_kwargs,
                )
            )

        return rval

    def find_expectation_indexes(self,
        expectation_type=None,
        column=None,
        expectation_kwargs=None
    ):
        """Find matching expectations within _expectation_config.
        Args:
            expectation_type=None                : The name of the expectation type to be matched.
            column=None                          : The name of the column to be matched.
            expectation_kwargs=None              : A dictionary of kwargs to match against.
        
        Returns:
            A list of indexes for matching expectation objects.
            If there are no matches, the list will be empty.
        """
        if expectation_kwargs == None:
            expectation_kwargs = {}

        if "column" in expectation_kwargs and column != None and column != expectation_kwargs["column"]:
            raise ValueError("Conflicting column names in remove_expectation: %s and %s" % (column, expectation_kwargs["column"]))

        if column != None:
            expectation_kwargs["column"] = column

        match_indexes = []
        for i, exp in enumerate(self._expectations_config.expectations):
            if expectation_type == None or (expectation_type == exp['expectation_type']):
                # if column == None or ('column' not in exp['kwargs']) or (exp['kwargs']['column'] == column) or (exp['kwargs']['column']==:
                match = True
                
                for k,v in expectation_kwargs.items():
                    if k in exp['kwargs'] and exp['kwargs'][k] == v:
                        continue
                    else:
                        match = False

                if match:
                    match_indexes.append(i)

        return match_indexes

    def find_expectations(self,
        expectation_type=None,
        column=None,
        expectation_kwargs=None,
        discard_output_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        """Find matching expectations within _expectation_config.
        Args:
            expectation_type=None                : The name of the expectation type to be matched.
            column=None                          : The name of the column to be matched.
            expectation_kwargs=None              : A dictionary of kwargs to match against.
            discard_output_format_kwargs=True    : In returned expectation object(s), suppress the `output_format` parameter.
            discard_include_configs_kwargs=True  : In returned expectation object(s), suppress the `include_configs` parameter.
            discard_catch_exceptions_kwargs=True : In returned expectation object(s), suppress the `catch_exceptions` parameter.
        
        Returns:
            A list of matching expectation objects.
            If there are no matches, the list will be empty.
        """

        match_indexes = self.find_expectation_indexes(
            expectation_type,
            column,
            expectation_kwargs,
        )

        return self._copy_and_clean_up_expectations_from_indexes(
            match_indexes,
            discard_output_format_kwargs,
            discard_include_configs_kwargs,
            discard_catch_exceptions_kwargs,
        )

    def remove_expectation(self,
        expectation_type=None,
        column=None,
        expectation_kwargs=None,
        remove_multiple_matches=False,
        dry_run=False,
    ):
        """Remove matching expectation(s) from _expectation_config.
        Args:
            expectation_type=None                : The name of the expectation type to be matched.
            column=None                          : The name of the column to be matched.
            expectation_kwargs=None              : A dictionary of kwargs to match against.
            remove_multiple_matches=False        : Match multiple expectations
            dry_run=False                        : Return a list of matching expectations without removing
        
        Returns:
            None, unless dry_run=True.
            If dry_run=True and remove_multiple_matches=False then return the expectation that *would be* removed.
            If dry_run=True and remove_multiple_matches=True then return a list of expectations that *would be* removed.

        Note:
            If remove_expectation doesn't find any matches, it raises a ValueError.
            If remove_expectation finds more than one matches and remove_multiple_matches!=True, it raises a ValueError.
            If dry_run=True, then `remove_expectation` acts as a thin layer to find_expectations, with the default values for discard_output_format_kwargs, discard_include_configs_kwargs, and discard_catch_exceptions_kwargs
        """

        match_indexes = self.find_expectation_indexes(
            expectation_type,
            column,
            expectation_kwargs,
        )

        if len(match_indexes) == 0:
            raise ValueError('No matching expectation found.')

        elif len(match_indexes) > 1:
            if not remove_multiple_matches:
                raise ValueError('Multiple expectations matched arguments. No expectations removed.')
            else:

                if not dry_run:
                    self._expectations_config.expectations = [i for j, i in enumerate(self._expectations_config.expectations) if j not in match_indexes]
                else:
                    return self._copy_and_clean_up_expectations_from_indexes(match_indexes)

        else: #Exactly one match
            expectation = self._copy_and_clean_up_expectation(
                self._expectations_config.expectations[match_indexes[0]]
            )

            if not dry_run:
                del self._expectations_config.expectations[match_indexes[0]]

            else:
                if remove_multiple_matches:
                    return [expectation]
                else:
                    return expectation

    def get_default_expectation_arguments(self):
        return self.default_expectation_args

    def set_default_expectation_argument(self, argument, value):
        #!!! Maybe add a validation check here?

        self.default_expectation_args[argument] = value

    def get_expectations_config(self,
        discard_failed_expectations=True,
        discard_output_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
        suppress_warnings=False
    ):        
        """Returns _expectation_config as a JSON object, and perform some cleaning along the way.
        Args:
            discard_failed_expectations=True     : Only include expectations with success_on_last_run=True in the exported config.
            discard_output_format_kwargs=True    : In returned expectation objects, suppress the `output_format` parameter.
            discard_include_configs_kwargs=True  : In returned expectation objects, suppress the `include_configs` parameter.
            discard_catch_exceptions_kwargs=True : In returned expectation objects, suppress the `catch_exceptions` parameter.
        
        Returns:
            An expectation config.

        Note:
            get_expectations_config does not affect the underlying config at all. The returned config is a copy of _expectations_config, not the original object.
        """
        config = dict(self._expectations_config)
        config = copy.deepcopy(config)
        expectations = config["expectations"]

        discards = defaultdict(int)

        if discard_failed_expectations:
            new_expectations = []

            for expectation in expectations:
                #Note: This is conservative logic.
                #Instead of retaining expectations IFF success==True, it discard expectations IFF success==False.
                #In cases where expectation["success"] is missing or None, expectations are *retained*.
                #Such a case could occur if expectations were loaded from a config file and never run.
                if "success_on_last_run" in expectation and expectation["success_on_last_run"] == False:
                    discards["failed_expectations"] += 1
                else:
                    new_expectations.append(expectation)

            expectations = new_expectations

        for expectation in expectations:
            #FIXME: Factor this out into a new function. The logic is duplicated in remove_expectation, which calls _copy_and_clean_up_expectation
            if "success_on_last_run" in expectation:
                del expectation["success_on_last_run"]

            if discard_output_format_kwargs:
                if "output_format" in expectation["kwargs"]:
                    del expectation["kwargs"]["output_format"]
                    discards["output_format"] += 1

            if discard_include_configs_kwargs:
                if "include_configs" in expectation["kwargs"]:
                    del expectation["kwargs"]["include_configs"]
                    discards["include_configs"] += 1

            if discard_catch_exceptions_kwargs:
                if "catch_exceptions" in expectation["kwargs"]:
                    del expectation["kwargs"]["catch_exceptions"]
                    discards["catch_exceptions"] += 1


        if not suppress_warnings:
            """
WARNING: get_expectations_config discarded
    12 failing expectations
    44 output_format kwargs
     0 include_config kwargs
     1 catch_exceptions kwargs
If you wish to change this behavior, please set discard_failed_expectations, discard_output_format_kwargs, discard_include_configs_kwargs, and discard_catch_exceptions_kwargs appropirately.
            """
            if any([discard_failed_expectations, discard_output_format_kwargs, discard_include_configs_kwargs, discard_catch_exceptions_kwargs]):
                print ("WARNING: get_expectations_config discarded")
                if discard_failed_expectations:
                    print ("\t%d failing expectations" % discards["failed_expectations"])
                if discard_output_format_kwargs:
                    print ("\t%d output_format kwargs" % discards["output_format"])
                if discard_include_configs_kwargs:
                    print ("\t%d include_configs kwargs" % discards["include_configs"])
                if discard_catch_exceptions_kwargs:
                    print ("\t%d catch_exceptions kwargs" % discards["catch_exceptions"])
                print ("If you wish to change this behavior, please set discard_failed_expectations, discard_output_format_kwargs, discard_include_configs_kwargs, and discard_catch_exceptions_kwargs appropirately.")

        config["expectations"] = expectations
        return config

    def save_expectations_config(
        self,
        filepath=None,
        discard_failed_expectations=True,
        discard_output_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
        suppress_warnings=False
    ):
        if filepath==None:
            #FIXME: Fetch the proper filepath from the project config
            pass

        expectations_config = self.get_expectations_config(
            discard_failed_expectations,
            discard_output_format_kwargs,
            discard_include_configs_kwargs,
            discard_catch_exceptions_kwargs,
            suppress_warnings
        )
        expectation_config_str = json.dumps(expectations_config, indent=2)
        open(filepath, 'w').write(expectation_config_str)

    def validate(self, expectations_config=None, catch_exceptions=True, output_format=None, include_config=None, only_return_failures=False):
        results = []

        if expectations_config is None:
            expectations_config = self.get_expectations_config(
                discard_failed_expectations=False,
                discard_output_format_kwargs=False,
                discard_include_configs_kwargs=False,
                discard_catch_exceptions_kwargs=False,
            )

        # Warn if our version is different from the version in the configuration
        try:
            if expectations_config['meta']['great_expectations.__version__'] != __version__:
                warnings.warn("WARNING: This configuration object was built using a different version of great_expectations than is currently validating it.")
        except KeyError:
            warnings.warn("WARNING: No great_expectations version found in configuration object.")

        for expectation in expectations_config['expectations']:
            expectation_method = getattr(self, expectation['expectation_type'])

            if output_format is not None:
                expectation['kwargs'].update({"output_format": output_format})

            if include_config is not None:
                expectation['kwargs'].update({"include_config": include_config})

            result = expectation_method(
                catch_exceptions=catch_exceptions,
                **expectation['kwargs']
            )

            if output_format != "BOOLEAN_ONLY":
                results.append(
                    dict(list(expectation.items()) + list(result.items()))
                )
            else:
                results.append(
                    dict(list(expectation.items()) + [("success", result)])
                )

        if only_return_failures:
            abbrev_results = []
            for exp in results:
                if exp["success"]==False:
                    abbrev_results.append(exp)
            results = abbrev_results

        return {
            "results" : results
        }


    ##### Output generation #####
    def format_column_map_output(self,
        output_format, success,
        element_count,
        nonnull_values, nonnull_count,
        boolean_mapped_success_values, success_count,
        exception_list, exception_index_list
    ):
        if output_format == "BOOLEAN_ONLY":
            return_obj = success

        elif output_format == "BASIC":
            exception_count = len(exception_list)

            if nonnull_count > 0:
                exception_percent = float(exception_count) / element_count
                exception_percent_nonmissing = float(exception_count) / nonnull_count
            else:
                exception_percent = None
                exception_percent_nonmissing = None

            return_obj = {
                "success": success,
                "summary_obj": {
                    "partial_exception_list": exception_list[:20],
                    "exception_count": exception_count,
                    "exception_percent": exception_percent,
                    "exception_percent_nonmissing": exception_percent_nonmissing,
                    # "exception_percent": excefloat(exception_count) / nonnull_count,
                }
            }

        elif output_format == "COMPLETE":
            return_obj = {
                "success": success,
                "exception_list": exception_list,
                "exception_index_list": exception_index_list,
            }

        elif output_format == "SUMMARY":
            # element_count = int(len(series))
            missing_count = element_count-int(len(nonnull_values))#int(null_indexes.sum())
            exception_count = len(exception_list)

            exception_value_series = pd.Series(exception_list).value_counts().iloc[:20]
            partial_exception_counts = dict(zip(
                list(exception_value_series.index),
                list(exception_value_series.values),
            ))

            if element_count > 0:
                missing_percent = float(missing_count) / element_count
                exception_percent = float(exception_count) / element_count

                if nonnull_count > 0:
                    exception_percent_nonmissing = float(exception_count) / nonnull_count
                else:
                    exception_percent_nonmissing = None

            else:
                missing_percent = None
                exception_percent = None
                exception_percent_nonmissing = None

            return_obj = {
                "success": success,
                "summary_obj": {
                    "element_count": element_count,
                    "missing_count": missing_count,
                    "missing_percent": missing_percent,
                    "exception_count": exception_count,
                    "exception_percent": exception_percent,
                    "exception_percent_nonmissing": exception_percent_nonmissing,
                    "partial_exception_counts": partial_exception_counts,
                    "partial_exception_list": exception_list[:20],
                    "partial_exception_index_list": exception_index_list[:20],
                }
            }

        else:
            print ("Warning: Unknown output_format %s. Defaulting to BASIC." % (output_format,))
            return_obj = {
                "success" : success,
                "exception_list" : exception_list,
            }

        return return_obj

    def calc_map_expectation_success(self, success_count, nonnull_count, mostly):
        if nonnull_count > 0:
            percent_success = float(success_count)/nonnull_count

            if mostly:
                success = bool(percent_success >= mostly)

            else:
                success = bool(nonnull_count-success_count == 0)

        else:
            success = True
            percent_success = None

        return success, percent_success

    ##### Iterative testing for custom expectations #####

    def test_expectation_function(self, function, *args, **kwargs):
        """Test a generic expectation function
        Args:
            function (func): The function to be tested. (Must be a valid expectation function.)
            *args          : Positional arguments to be passed the the function
            **kwargs       : Keyword arguments to be passed the the function
        
        Returns:
            A JSON-serializable expectation result object.

        Notes:
            This function is a thin layer to allow quick testing of new expectation functions, without having to define custom classes, etc.
            To use developed expectations from the command-line tool, you'll still need to define custom classes, etc.
        """

        new_function = self.expectation(inspect.getargspec(function)[0][1:])(function)
        return new_function(self, *args, **kwargs)

    def test_column_map_expectation_function(self, function, *args, **kwargs):
        """Test a column map expectation function
        Args:
            function (func): The function to be tested. (Must be a valid expectation function.)
            *args          : Positional arguments to be passed the the function
            **kwargs       : Keyword arguments to be passed the the function
        
        Returns:
            A JSON-serializable expectation result object.

        Notes:
            This function is a thin layer to allow quick testing of new expectation functions, without having to define custom classes, etc.
            To use developed expectations from the command-line tool, you'll still need to define custom classes, etc.
        """

        new_function = self.column_map_expectation( function )
        return new_function(self, *args, **kwargs)

    def test_column_aggregate_expectation_function(self, function, *args, **kwargs):
        """Test a column aggregate expectation function
        Args:
            function (func): The function to be tested. (Must be a valid expectation function.)
            *args          : Positional arguments to be passed the the function
            **kwargs       : Keyword arguments to be passed the the function
        
        Returns:
            A JSON-serializable expectation result object.

        Notes:
            This function is a thin layer to allow quick testing of new expectation functions, without having to define custom classes, etc.
            To use developed expectations from the command-line tool, you'll still need to define custom classes, etc.
        """

        new_function = self.column_aggregate_expectation( function )
        return new_function(self, *args, **kwargs)

    ##### Table shape expectations #####

    def expect_column_to_exist(self, column,
                               output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the specified column to exist in the data set.
        Args:
            column (str): The column name.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }

        """
        raise NotImplementedError

    def expect_table_row_count_to_be_between(self, min_value=0, max_value=None,
                                             output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the number of rows in a data set to be between two values.
        Args:
            min_value (int or None): the minimum number of rows.
            max_value (int or None): the maximum number of rows.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_table_row_count_to_equal
        """
        raise NotImplementedError

    def expect_table_row_count_to_equal(self, value=None,
                                        output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the number of rows to be equal to a value.
        Args:
	        value (int): The value that should equal the number of rows.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_table_row_count_to_be_between
        """
        raise NotImplementedError

    ##### Missing values, unique values, and types #####

    def expect_column_values_to_be_unique(self, column,
                                          mostly=None,
                                          output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect each nonempty column entry to be unique (no duplicates).
        Args:
            column (str): The column name.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of unique values is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }

        Examples:
	    Display multiple duplicated items. For example, ['2','2','2'] will return `['2','2']` for the exceptions_list.
        """
        raise NotImplementedError

    def expect_column_values_to_not_be_null(self, column,
                                            mostly=None,
                                            output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect each column entry to be nonempty.
        Args:
            column (str): The column name.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of not null values is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_null

        """
        raise NotImplementedError

    def expect_column_values_to_be_null(self, column,
                                        mostly=None,
                                        output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the column entries to be empty.
        Args:
            column (str): The column name.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of null values is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_be_null

        """
        raise NotImplementedError

    def expect_column_values_to_be_of_type(self, column, type_, target_datasource="numpy",
                                           mostly=None,
                                           output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect each column entry to be a specified data type.
        Args:
            column (str): The column name.
            type_ (str): A string representing the data type that each column should have as entries.
                For example, "double integer" refers to an integer with double precision.
            target_datasource (str): The data source that specifies the implementation in the type_ parameter.
                For example, options include "numpy", "sql", or "spark".
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values of type_ is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }

        """
        raise NotImplementedError

    def expect_column_values_to_be_in_type_list(self, column, type_list, target_datasource="numpy",
                                                mostly=None,
                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect each column entry to be a specified data type.
        Args:
            column (str): The column name.
            type_list (list of str): A list of strings representing the data type that each column should have as entries.
                For example, "double integer" refers to an integer with double precision.
            target_datasource (str): The data source that specifies the implementation in the type_ parameter.
                For example, options include "numpy", "sql", or "spark".
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values of type_ is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }

        """
        raise NotImplementedError

    ##### Sets and ranges #####

    def expect_column_values_to_be_in_set(self, column, values_set,
                                          mostly=None,
                                          output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect each entry in a column to be in a given set.
        Args:
            column (str): The column name.
            values_set (set-like): The set of objects or unique data points corresponding to the column.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values in values_set is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_be_in_set

        """
        raise NotImplementedError

    def expect_column_values_to_not_be_in_set(self, column, values_set,
                                              mostly=None,
                                              output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to not be in the set.
        Args:
            column (str): The column name.
            values_set (list): The set of objects or unique data points that should not correspond to the column.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values not in values_set is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_be_in_set

        """
        raise NotImplementedError

    def expect_column_values_to_be_between(self, column, min_value=None, max_value=None, parse_strings_as_datetimes=None,
                                           mostly=None,
                                           output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to be a number between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for a column entry.
            max_value (int or None): The maximum value for a column entry.
            parse_strings_as_datetimes (boolean or None) : If True, parse min_value, max_values, and all non-null column values to datetimes before making comparisons.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values between min_value and max_value is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_value_lengths_to_be_between

        """
        raise NotImplementedError

    def expect_column_values_to_be_increasing(self, column, strictly=None, parse_strings_as_datetimes=None,
                                              mostly=None,
                                              output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column values to be increasing. (Only works for numeric data.)
        Args:
            column (str): The column name.
            strictly (Boolean or None): If True, values must be strictly greater than previous values
            parse_strings_as_datetimes (boolean or None) : If True, all non-null column values to datetimes before making comparisons
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_decreasing

        """
        raise NotImplementedError

    def expect_column_values_to_be_decreasing(self, column, strictly=None, parse_strings_as_datetimes=None,
                                              mostly=None,
                                              output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column values to be decreasing. (Only works for numeric data.)
        Args:
            column (str): The column name.
            strictly (Boolean or None): If True, values must be strictly less than than previous values
            parse_strings_as_datetimes (boolean or None) : If True, all non-null column values to datetimes before making comparisons
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_increasing

        """
        raise NotImplementedError


    ##### String matching #####

    def expect_column_value_lengths_to_be_between(self, column, min_value=None, max_value=None,
                                                  mostly=None,
                                                  output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to have a measurable length which lies between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for a column entry length.
            max_value (int or None): The maximum value for a column entry length.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of value lengths between min_value and max_value is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_between

        """
        raise NotImplementedError

    def expect_column_value_lengths_to_equal(self, column, value,
                                             mostly=None,
                                             output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to have a measurable length which is equal to the provided value.

        :param column (str): The column name.
        :param value (int): The expected value of the column length.
        :return: Column map expectation return object in accordance with output_format
        """

    def expect_column_values_to_match_regex(self, column, regex,
                                            mostly=None,
                                            output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to be strings that match a given regular expression.
        Args:
            column (str): The column name.
            regex (str): The regular expression that the column entry should match.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_match_regex
            expect_column_values_to_match_regex_list
        """
        raise NotImplementedError

    def expect_column_values_to_not_match_regex(self, column, regex,
                                                mostly=None,
                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to be strings that do NOT match a given regular expression.
        Args:
            column (str): The column name.
            regex (str): The regular expression that the column entry should NOT match.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of NOT matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_match_regex
            expect_column_values_to_match_regex_list
        """
        raise NotImplementedError

    def expect_column_values_to_match_regex_list(self, column, regex_list, match_on="any",
                                                 mostly=None,
                                                 output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the column entries to be strings that match at least one of a list of regular expressions.
        Args:
            column (str): The column name.
            regex_list (list): The list of regular expressions in which the column entries should match according to match_on.
        Keyword Args:
            match_on="any": Use "any" if the value should match at least one regular expression in the list. Use "all" if it should match each regular expression in the list.
            mostly=None: Return "success": True if the percentage of matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_match_regex
            expect_column_values_to_not_match_regex
        """
        raise NotImplementedError

    ##### Datetime and JSON parsing #####

    def expect_column_values_to_match_strftime_format(self, column, strftime_format,
                                                      mostly=None,
                                                      output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to be strings representing a date or time with a given format.
        WARNING: Note that strftime formats are not universally portable across implementations.
        For example, the %z directive may not be implemented before Python 3.2.
        Args:
            column (str): The column name.
            strftime_format (str): The datetime format that the column entries should match.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        """
        raise NotImplementedError

    def expect_column_values_to_be_dateutil_parseable(self, column,
                                                      mostly=None,
                                                      output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to be interpretable as a dateutil object.
        Args:
            column (str): The column name.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of parseable values is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        """
        raise NotImplementedError

    def expect_column_values_to_be_json_parseable(self, column,
                                                  mostly=None,
                                                  output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to be data written in JavaScript Object Notation.
        Args:
            column (str): The column name.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_match_json_schema
        """
        raise NotImplementedError

    def expect_column_values_to_match_json_schema(self, column, json_schema,
                                                  mostly=None,
                                                  output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect column entries to be JSON objects with a given JSON schema.
        Args:
            column (str): The column name.
            json_schema (JSON object): The JSON schema that each column entry should resemble.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_json_parseable

            The JSON-schema docs at: http://json-schema.org/
        """
        raise NotImplementedError

    ##### Aggregate functions #####

    def expect_column_mean_to_be_between(self, column, min_value=None, max_value=None,
                                         output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the column mean to be between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for the column mean.
            max_value (int or None): The maximum value for the column mean.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_mean": (float) the column mean
                }
        """
        raise NotImplementedError

    def expect_column_median_to_be_between(self, column, min_value=None, max_value=None,
                                           output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the column median to be between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for the column median.
            max_value (int or None): The maximum value for the column median.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_median": (float) the column median
                }
        """
        raise NotImplementedError

    def expect_column_stdev_to_be_between(self, column, min_value=None, max_value=None,
                                          output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the column standard deviation to be between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for the column standard deviation.
            max_value (int or None): The maximum value for the column standard deviation.

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_stdev": (float) the true standard deviation
                }
        """
        raise NotImplementedError

    def expect_column_unique_value_count_to_be_between(self, column, min_value=None, max_value=None,
                                                       output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the number of unique values to be between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (int or None): The minimum number of unique values. If None, then there is no minimium expected value.
            max_value (int or None): The maximum number of unique values. If None, then there is no maximum expected value.

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_value": (float) the column mean
                }
        """
        raise NotImplementedError

    def expect_column_proportion_of_unique_values_to_be_between(self, column, min_value=0, max_value=1,
                                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the proportion of unique values to be between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (float or None): The minimum proportion of unique values. (Proportions are on the range 0 to 1)
            max_value (float or None): The maximum proportion of unique values. (Proportions are on the range 0 to 1)

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_value": (float) the proportion of unique values
                }
        """
        raise NotImplementedError

    def expect_column_most_common_value_to_be(self, column, value, ties_okay=None,
                                              output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the most common value to be equal to `value`

        Args:
            column (str): The column name.
            value  (any): The value
            ties_okay (boolean or None): If True, then the expectation will succeed if other values are as common (but not more common) than the selected value

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_value": (float) the proportion of unique values
                }
        """
        raise NotImplementedError

    def expect_column_most_common_value_to_be_in_set(self, column, value_set, ties_okay=None,
                                                     output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the most common value to be within the designated value set

        Args:
            column (str): The column name.
            value_set (list): The lis of designated values
            ties_okay (boolean or None): If True, then the expectation will succeed if other values are as common (but not more common) than the selected value

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_value": (float) the proportion of unique values
                }
        """
        raise NotImplementedError


    ### Distributional expectations
    def expect_column_chisquare_test_p_value_to_be_greater_than(self, column, partition_object=None, p=0.05, tail_weight_holdout=0,
                                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """
        Expect the values in this column to match the distribution of the specified categorical values and their expected weights. \
        The expected distribution is calculated by scaling the weights according to the size of values in the test data.

        :param column (str): The column name
        :param partition_object (dict): A dictionary containing partition (categorical values) and associated weights.
        :param p (float) = 0.05: The p-value threshold for the Chi-Squared test.\
            For values below the specified threshold the expectation will return false, rejecting the null hypothesis that the distributions are the same.
        :param tail_weight_holdout: the amount of weight to split uniformly and add to the tails of the histogram (the area between -Infinity and the data's min value and between the data's max value and Infinity)

        :return: result_obj including:
        result_obj including:
            "success": (Boolean) True if the column passed the expectation
            "true_value": (float) The true KL divergence (relative entropy)
            "summary_obj": {
                "observed_partition": The partition observed in the data
                "expected_partition": The partition against which the data were compared, after applying specified holdouts.
            }
        """
        raise NotImplementedError

    def expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(self, column, partition_object=None, p=0.05, bootstrap_samples=None, bootstrap_sample_size=None,
                                                                      output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the values in this column to match the distribution implied by the specified partition. \
        The expected CDF is constructed as a linear interpolation between the bins, using the provided weights.

        :param column: The column name to test
        :param partition_object: The expected partition object.
        :param p: The p-value threshold for the Kolmogorov-Smirnov test.\
            For values below the specified threshold the expectation will return false, rejecting the null hypothesis that the distributions are the same.
        :param bootstrap_samples: The number of times to bootstrap. If None, defaults to 1000.
        :param bootstrap_sample_size: The number of samples per bootstrap. If None, defaults to 2 * len(partition_object['weights'])\
            A larger sample will increase the specificity of the test.

        :return: result_obj including:
            "success": (Boolean) True if the column passed the expectation
            "true_value": (float) The true p-value of the KS test
            "summary_obj": {
                "bootstrap_samples": The number of bootstrap samples used
                "bootstrap_sample_size": The number of samples taken from the column in each bootstrap sample
                "observed_cdf": The cumulative density function observed in the data, a dict containing 'x' values and cdf_values (suitable for plotting)
                "expected_cdf": The cumulative density function expected based on the partition object, a dict containing 'x' values and cdf_values (suitable for plotting)
                "observed_partition": The partition observed on the data, using the provided bins but also expanding from min(column) to max(column)
                "expected_partition": The partition expected from the data. For KS test, this will always be the partition_object parameter
            }
        """
        raise NotImplementedError

    def expect_column_kl_divergence_to_be_less_than(self, column, partition_object=None, threshold=None, tail_weight_holdout=0, internal_weight_holdout=0,
                                                    output_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the Kulback-Leibler divergence (relative entropy) of the specified column and the partition object to be lower than the provided threshold.
        When KL divergence is zero, the datasets are both distributed identically (according to the provided partition).
        In many practical contexts, choosing a value between 0.5 and 1 will provide a useful test.

        If the partition_object is categorical, this expectation will expect the values in column to also be categorical.
            * If the column includes values that are not present in the partition, the tail_weight_holdout will be equally split among those values, providing a mechanism to weaken the strictness of the expectation (otherwise, relative entropy would immediately go to infinity).
            * If the partition includes values that are not present in the column, the test will simply include zero weight for that value.
        If the partition_object is continuous, this expectation will discretize the values in the column according to the bins specified in the partition_object, and apply the test to the resulting distribution.
            * The internal_weight_holdout and tail_weight_holdout parameters provide a mechanism to weaken the expectation, since an expected weight of zero would drive relative entropy to be infinite if any data are observed in that interval.
            * If internal_weight_holdout is specified, that value will be distributed equally among any intervals with weight zero in the partition_object.
            * If tail_weight_holdout is specified, that value will be appended to the tails of the bins ((-Infinity, min(bins)) and (max(bins), Infinity).

        :param column: the column to which to apply the expectation
        :param partition_object: the partition_object with which to compare the data in column
        :param threshold: the threshold below which the test should be considered to have passed
        :param internal_weight_holdout: the amount of weight to split uniformly among zero-weighted partition elements in a given partition.
        :param tail_weight_holdout: the amount of weight to split uniformly and add to the tails of the histogram (the area between -Infinity and the data's min value and between the data's max value and Infinity)

        :return: result_obj including:
        result_obj including:
            "success": (Boolean) True if the column passed the expectation
            "true_value": (float) The true KL divergence (relative entropy)
            "summary_obj": {
                "observed_partition": The partition observed in the data
                "expected_partition": The partition against which the data were compared, after applying specified holdouts.
            }
        """
        raise NotImplementedError
