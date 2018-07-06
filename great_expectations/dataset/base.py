from __future__ import division
import json
import inspect
import copy
from functools import wraps
import traceback
import warnings
from six import string_types
from collections import namedtuple

from collections import (
    Counter,
    defaultdict
)

from ..version import __version__
from .util import DotDict, recursively_convert_to_json_serializable, parse_result_format


class Dataset(object):

    def __init__(self, *args, **kwargs):
        super(Dataset, self).__init__(*args, **kwargs)
        self._initialize_expectations()

    @classmethod
    def expectation(cls, method_arg_names):
        """Manages configuration and running of expectation objects.

        Expectation builds and saves a new expectation configuration to the Dataset object. It is the core decorator \
        used by great expectations to manage expectation configurations.

        Args:
            method_arg_names (List) : An ordered list of the arguments used by the method implementing the expectation \
                (typically the result of inspection). Positional arguments are explicitly mapped to \
                keyword arguments when the expectation is run.

        Notes:
            Intermediate decorators that call the core @expectation decorator will most likely need to pass their \
            decorated methods' signature up to the expectation decorator. For example, the MetaPandasDataset \
            column_map_expectation decorator relies on the Dataset expectation decorator, but will pass through the \
            signature from the implementing method.

            @expectation intercepts and takes action based on the following parameters:
                * include_config (boolean or None) : \
                    If True, then include the generated expectation config as part of the result object. \
                    For more detail, see :ref:`include_config`.
                * catch_exceptions (boolean or None) : \
                    If True, then catch exceptions and include them as part of the result object. \
                    For more detail, see :ref:`catch_exceptions`.
                * result_format (str or None) : \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                    For more detail, see :ref:`result_format <result_format>`.
                * meta (dict or None): \
                    A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                    For more detail, see :ref:`meta`.
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

                if "result_format" in kwargs:
                    result_format = kwargs["result_format"]
                else:
                    result_format = self.default_expectation_args["result_format"]

                # Extract the meta object for use as a top-level expectation_config holder
                if "meta" in kwargs:
                    meta = kwargs["meta"]
                    del all_args["meta"]
                else:
                    meta = None

                # This intends to get the signature of the inner wrapper, if there is one.
                if "result_format" in inspect.getargspec(func)[0][1:]:
                    all_args["result_format"] = result_format
                else:
                    if "result_format" in all_args:
                        del all_args["result_format"]

                all_args = recursively_convert_to_json_serializable(all_args)

                # Patch in PARAMETER args, and remove locally-supplied arguments
                expectation_args = copy.deepcopy(all_args) # This will become the stored config

                if "evaluation_parameters" in self._expectations_config:
                    evaluation_args = self._build_evaluation_parameters(expectation_args,
                                                                        self._expectations_config["evaluation_parameters"]) # This will be passed to the evaluation
                else:
                    evaluation_args = self._build_evaluation_parameters(expectation_args, None)

                #Construct the expectation_config object
                expectation_config = DotDict({
                    "expectation_type": method_name,
                    "kwargs": expectation_args
                })

                # Add meta to our expectation_config
                if meta is not None:
                    expectation_config["meta"] = meta

                raised_exception = False
                exception_traceback = None
                exception_message = None

                # Finally, execute the expectation method itself
                try:
                    return_obj = func(self, **evaluation_args)

                except Exception as err:
                    if catch_exceptions:
                        raised_exception = True
                        exception_traceback = traceback.format_exc()
                        exception_message = str(err)

                        return_obj = {
                            "success": False
                        }

                    else:
                        raise(err)

                # Append the expectation to the config.
                self._append_expectation(expectation_config)

                if include_config:
                    return_obj["expectation_config"] = copy.deepcopy(expectation_config)

                if catch_exceptions:
                    return_obj["exception_info"] = {
                        "raised_exception": raised_exception,
                        "exception_message": exception_message,
                        "exception_traceback": exception_traceback
                    }

                # Add a "success" object to the config
                expectation_config["success_on_last_run"] = return_obj["success"]

                return_obj = recursively_convert_to_json_serializable(return_obj)
                return return_obj

            return wrapper

        return outer_wrapper

    @classmethod
    def column_map_expectation(cls, func):
        """Constructs an expectation using column-map semantics.

        The column_map_expectation decorator handles boilerplate issues surrounding the common pattern of evaluating
        truthiness of some condition on a per-row basis.

        Args:
            func (function): \
                The function implementing a row-wise expectation. The function should take a column of data and \
                return an equally-long column of boolean values corresponding to whether the truthiness of the \
                underlying expectation.

        Notes:
            column_map_expectation intercepts and takes action based on the following parameters:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

            column_map_expectation *excludes null values* from being passed to the function

            Depending on the `result_format` selected, column_map_expectation can additional data to a return object, \
            including `element_count`, `nonnull_values`, `nonnull_count`, `success_count`, `unexpected_list`, and \
            `unexpected_index_list`. See :func:`_format_column_map_output <great_expectations.dataset.base.Dataset._format_column_map_output>`

        See also:
            :func:`expect_column_values_to_be_unique <great_expectations.dataset.base.Dataset.expect_column_values_to_be_unique>` \
            for an example of a column_map_expectation
        """
        raise NotImplementedError

    @classmethod
    def column_aggregate_expectation(cls, func):
        """Constructs an expectation using column-aggregate semantics.

        The column_aggregate_expectation decorator handles boilerplate issues surrounding the common pattern of \
        evaluating truthiness of some condition on an aggregated-column basis.

        Args:
            func (function): \
                The function implementing an expectation using an aggregate property of a column. \
                The function should take a column of data and return the aggregate value it computes.

        Notes:
            column_aggregate_expectation *excludes null values* from being passed to the function

        See also:
            :func:`expect_column_mean_to_be_between <great_expectations.dataset.base.Dataset.expect_column_mean_to_be_between>` \
            for an example of a column_aggregate_expectation
        """
        raise NotImplementedError

    def _initialize_expectations(self, config=None, name=None):
        """Instantiates `_expectations_config` as empty by default or with a specified expectation `config`.
        In addition, this always sets the `default_expectation_args` to:
            `include_config`: False,
            `catch_exceptions`: False,
            `output_format`: 'BASIC'

        Args:
            config (json): \
                A json-serializable expectation config. \
                If None, creates default `_expectations_config` with an empty list of expectations and \
                key value `dataset_name` as `name`.

            name (string): \
                The name to assign to `_expectations_config.dataset_name` if `config` is not provided.

        """
        if config != None:
            #!!! Should validate the incoming config with jsonschema here

            # Copy the original so that we don't overwrite it by accident
            ## Pandas incorrectly interprets this as an attempt to create a column and throws up a warning. Suppress it
            ## since we are subclassing.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                self._expectations_config = DotDict(copy.deepcopy(config))

        else:
            ## Pandas incorrectly interprets this as an attempt to create a column and throws up a warning. Suppress it
            ## since we are subclassing.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                self._expectations_config = DotDict({
                    "dataset_name" : name,
                    "meta": {
                        "great_expectations.__version__": __version__
                    },
                    "expectations" : []
                })

        ## Pandas incorrectly interprets this as an attempt to create a column and throws up a warning. Suppress it
        ## since we are subclassing.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            self.default_expectation_args = {
                "include_config" : False,
                "catch_exceptions" : False,
                "result_format" : 'BASIC',
            }

    def _append_expectation(self, expectation_config):
        """Appends an expectation to `DataSet._expectations_config` and drops existing expectations of the same type.

           If `expectation_config` is a column expectation, this drops existing expectations that are specific to \
           that column and only if it is the same expectation type as `expectation_config`. Otherwise, if it's not a \
           column expectation, this drops existing expectations of the same type as `expectation config`. \
           After expectations of the same type are dropped, `expectation_config` is appended to `DataSet._expectations_config`.

           Args:
               expectation_config (json): \
                   The JSON-serializable expectation to be added to the DataSet expectations in `_expectations_config`.

           Notes:
               May raise future errors once json-serializable tests are implemented to check for correct arg formatting

        """
        expectation_type = expectation_config['expectation_type']

        #Test to ensure the new expectation is serializable.
        #FIXME: If it's not, are we sure we want to raise an error?
        #FIXME: Should we allow users to override the error?
        #FIXME: Should we try to convert the object using something like recursively_convert_to_json_serializable?
        json.dumps(expectation_config)

        #Drop existing expectations with the same expectation_type.
        #For column_expectations, _append_expectation should only replace expectations
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
        discard_result_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        """Returns copy of `expectation` without `success_on_last_run` and other specified key-value pairs removed

          Returns a copy of specified expectation will not have `success_on_last_run` key-value. The other key-value \
          pairs will be removed by default but will remain in the copy if specified.

          Args:
              expectation (json): \
                  The expectation to copy and clean.
              discard_result_format_kwargs (boolean): \
                  if True, will remove the kwarg `output_format` key-value pair from the copied expectation.
              discard_include_configs_kwargs (boolean):
                  if True, will remove the kwarg `include_configs` key-value pair from the copied expectation.
              discard_catch_exceptions_kwargs (boolean):
                  if True, will remove the kwarg `catch_exceptions` key-value pair from the copied expectation.

          Returns:
              A copy of the provided expectation with `success_on_last_run` and other specified key-value pairs removed
        """
        new_expectation = copy.deepcopy(expectation)

        if "success_on_last_run" in new_expectation:
            del new_expectation["success_on_last_run"]

        if discard_result_format_kwargs:
            if "result_format" in new_expectation["kwargs"]:
                del new_expectation["kwargs"]["result_format"]
                # discards["result_format"] += 1

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
        discard_result_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        """Copies and cleans all expectations provided by their index in DataSet._expectations_config.expectations.

           Applies the _copy_and_clean_up_expectation method to multiple expectations, provided by their index in \
           `DataSet,_expectations_config.expectations`. Returns a list of the copied and cleaned expectations.

           Args:
               match_indexes (List): \
                   Index numbers of the expectations from `expectation_config.expectations` to be copied and cleaned.
               discard_result_format_kwargs (boolean): \
                   if True, will remove the kwarg `output_format` key-value pair from the copied expectation.
               discard_include_configs_kwargs (boolean):
                   if True, will remove the kwarg `include_configs` key-value pair from the copied expectation.
               discard_catch_exceptions_kwargs (boolean):
                   if True, will remove the kwarg `catch_exceptions` key-value pair from the copied expectation.

           Returns:
               A list of the copied expectations with `success_on_last_run` and other specified \
               key-value pairs removed.

           See also:
               _copy_and_clean_expectation
        """
        rval = []
        for i in match_indexes:
            rval.append(
                self._copy_and_clean_up_expectation(
                    self._expectations_config.expectations[i],
                    discard_result_format_kwargs,
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
        discard_result_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
    ):
        """Find matching expectations within _expectation_config.
        Args:
            expectation_type=None                : The name of the expectation type to be matched.
            column=None                          : The name of the column to be matched.
            expectation_kwargs=None              : A dictionary of kwargs to match against.
            discard_result_format_kwargs=True    : In returned expectation object(s), suppress the `result_format` parameter.
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
            discard_result_format_kwargs,
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
            If dry_run=True, then `remove_expectation` acts as a thin layer to find_expectations, with the default values for discard_result_format_kwargs, discard_include_configs_kwargs, and discard_catch_exceptions_kwargs
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


    def discard_failing_expectations(self):
        res = self.validate(only_return_failures=True).get('results')
        if any(res):
            for item in res:
                self.remove_expectation(expectation_type=item['expectation_config']['expectation_type'],
                                        expectation_kwargs=item['expectation_config']['kwargs'])
#            print("WARNING: Removed %s expectations that were 'False'" % len(res))
            warnings.warn("Removed %s expectations that were 'False'" % len(res))

    def get_default_expectation_arguments(self):
        """Fetch default expectation arguments for this dataset

        Returns:
            A dictionary containing all the current default expectation arguments for a dataset

            Ex::

                {
                    "include_config" : False,
                    "catch_exceptions" : False,
                    "result_format" : 'BASIC'
                }

        See also:
            set_default_expectation_arguments
        """
        return self.default_expectation_args

    def set_default_expectation_argument(self, argument, value):
        """Set a default expectation argument for this dataset

        Args:
            argument (string): The argument to be replaced
            value : The New argument to use for replacement

        Returns:
            None

        See also:
            get_default_expectation_arguments
        """
        #!!! Maybe add a validation check here?

        self.default_expectation_args[argument] = value

    def get_expectations_config(self,
        discard_failed_expectations=True,
        discard_result_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
        suppress_warnings=False
    ):
        """Returns _expectation_config as a JSON object, and perform some cleaning along the way.
        Args:
            discard_failed_expectations=True     : Only include expectations with success_on_last_run=True in the exported config.
            discard_result_format_kwargs=True    : In returned expectation objects, suppress the `result_format` parameter.
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

            if discard_result_format_kwargs:
                if "result_format" in expectation["kwargs"]:
                    del expectation["kwargs"]["result_format"]
                    discards["result_format"] += 1

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
    44 result_format kwargs
     0 include_config kwargs
     1 catch_exceptions kwargs
If you wish to change this behavior, please set discard_failed_expectations, discard_result_format_kwargs, discard_include_configs_kwargs, and discard_catch_exceptions_kwargs appropirately.
            """
            if any([discard_failed_expectations, discard_result_format_kwargs, discard_include_configs_kwargs, discard_catch_exceptions_kwargs]):
                print ("WARNING: get_expectations_config discarded")
                if discard_failed_expectations:
                    print ("\t%d failing expectations" % discards["failed_expectations"])
                if discard_result_format_kwargs:
                    print ("\t%d result_format kwargs" % discards["result_format"])
                if discard_include_configs_kwargs:
                    print ("\t%d include_configs kwargs" % discards["include_configs"])
                if discard_catch_exceptions_kwargs:
                    print ("\t%d catch_exceptions kwargs" % discards["catch_exceptions"])
                print ("If you wish to change this behavior, please set discard_failed_expectations, discard_result_format_kwargs, discard_include_configs_kwargs, and discard_catch_exceptions_kwargs appropirately.")

        config["expectations"] = expectations
        return config

    def save_expectations_config(
        self,
        filepath=None,
        discard_failed_expectations=True,
        discard_result_format_kwargs=True,
        discard_include_configs_kwargs=True,
        discard_catch_exceptions_kwargs=True,
        suppress_warnings=False
    ):
        """Writes ``_expectation_config`` to a JSON file.

           Writes the DataSet's expectation config to the specified JSON ``filepath``. Failing expectations \
           can be excluded from the JSON expectations config with ``discard_failed_expectations``. The kwarg key-value \
           pairs :ref:`result_format`, :ref:`include_config`, and :ref:`catch_exceptions` are optionally excluded from the JSON \
           expectations config.

           Args:
               filepath (string): \
                   The location and name to write the JSON config file to.
               discard_failed_expectations (boolean): \
                   If True, excludes expectations that do not return ``success = True``. \
                   If False, all expectations are written to the JSON config file.
               discard_result_format_kwargs (boolean): \
                   If True, the :ref:`result_format` attribute for each expectation is not written to the JSON config file. \
               discard_include_configs_kwargs (boolean): \
                   If True, the :ref:`include_config` attribute for each expectation is not written to the JSON config file.\
               discard_catch_exceptions_kwargs (boolean): \
                   If True, the :ref:`catch_exceptions` attribute for each expectation is not written to the JSON config \
                   file.
               suppress_warnings (boolean): \
                  It True, all warnings raised by Great Expectations, as a result of dropped expectations, are \
                  suppressed.

        """
        if filepath==None:
            #FIXME: Fetch the proper filepath from the project config
            pass

        expectations_config = self.get_expectations_config(
            discard_failed_expectations,
            discard_result_format_kwargs,
            discard_include_configs_kwargs,
            discard_catch_exceptions_kwargs,
            suppress_warnings
        )
        expectation_config_str = json.dumps(expectations_config, indent=2)
        open(filepath, 'w').write(expectation_config_str)

    def validate(self, expectations_config=None, evaluation_parameters=None, catch_exceptions=True, result_format=None, only_return_failures=False):
        """Generates a JSON-formatted report describing the outcome of all expectations.

            Use the default expectations_config=None to validate the expectations config associated with the DataSet.

            Args:
                expectations_config (json or None): \
                    If None, uses the expectations config generated with the Dataset during the current session. \
                    If a JSON file, validates those expectations.
                evaluation_parameters (dict or None): \
                    If None, uses the evaluation_paramters from the expectations_config provided or as part of the dataset.
                    If a dict, uses the evaluation parameters in the dictionary.
                catch_exceptions (boolean): \
                    If True, exceptions raised by tests will not end validation and will be described in the returned report.
                result_format (string or None): \
                    If None, uses the default value ('BASIC' or as specified). \
                    If string, the returned expectation output follows the specified format ('BOOLEAN_ONLY','BASIC', etc.).
                include_config (boolean): \
                    If True, the returned results include the config information associated with each expectation, if \
                    it exists.
                only_return_failures (boolean): \
                    If True, expectation results are only returned when ``success = False``\.

            Returns:
                A JSON-formatted dictionary containing a list of the validation results. \
                An example of the returned format::

                {
                  "results": [
                    {
                      "unexpected_list": [unexpected_value_1, unexpected_value_2],
                      "expectation_type": "expect_*",
                      "kwargs": {
                        "column": "Column_Name",
                        "output_format": "SUMMARY"
                      },
                      "success": true,
                      "raised_exception: false.
                      "exception_traceback": null
                    },
                    {
                      ... (Second expectation results)
                    },
                    ... (More expectations results)
                  ],
                  "success": true,
                  "statistics": {
                    "evaluated_expectations": n,
                    "successful_expectations": m,
                    "unsuccessful_expectations": n - m,
                    "success_percent": m / n
                  }
                }

           Notes:
               If the configuration object was built with a different version of great expectations then the current environment. \
               If no version was found in the configuration file.

           Raises:
               AttributeError - if 'catch_exceptions'=None and an expectation throws an AttributeError
        """
        results = []

        if expectations_config is None:
            expectations_config = self.get_expectations_config(
                discard_failed_expectations=False,
                discard_result_format_kwargs=False,
                discard_include_configs_kwargs=False,
                discard_catch_exceptions_kwargs=False,
            )
        elif isinstance(expectations_config, string_types):
            expectations_config = json.load(open(expectations_config, 'r'))

        if evaluation_parameters is None:
            # Use evaluation parameters from the (maybe provided) config
            if "evaluation_parameters" in expectations_config:
                evaluation_parameters = expectations_config["evaluation_parameters"]

        # Warn if our version is different from the version in the configuration
        try:
            if expectations_config['meta']['great_expectations.__version__'] != __version__:
                warnings.warn("WARNING: This configuration object was built using a different version of great_expectations than is currently validating it.")
        except KeyError:
            warnings.warn("WARNING: No great_expectations version found in configuration object.")

        for expectation in expectations_config['expectations']:
            try:
                expectation_method = getattr(self, expectation['expectation_type'])

                if result_format is not None:
                    expectation['kwargs'].update({"result_format": result_format})

                # A missing parameter should raise a KeyError
                evaluation_args = self._build_evaluation_parameters(expectation['kwargs'], evaluation_parameters)

                result = expectation_method(
                    catch_exceptions=catch_exceptions,
                    **evaluation_args
                )

            except Exception as err:
                if catch_exceptions:
                    raised_exception = True
                    exception_traceback = traceback.format_exc()

                    result = {
                        "success": False,
                        "exception_info": {
                            "raised_exception": raised_exception,
                            "exception_traceback": exception_traceback,
                            "exception_message": str(err)
                        }
                    }

                else:
                    raise(err)

            #if include_config:
            result["expectation_config"] = copy.deepcopy(expectation)

            # Add an empty exception_info object if no exception was caught
            if catch_exceptions and ('exception_info' not in result):
                result["exception_info"] = {
                    "raised_exception": False,
                    "exception_traceback": None,
                    "exception_message": None
                }

            results.append(result)

        statistics = _calc_validation_statistics(results)

        if only_return_failures:
            abbrev_results = []
            for exp in results:
                if exp["success"]==False:
                    abbrev_results.append(exp)
            results = abbrev_results

        result = {
            "results": results,
            "success": statistics.success,
            "statistics": {
                "evaluated_expectations": statistics.evaluated_expectations,
                "successful_expectations": statistics.successful_expectations,
                "unsuccessful_expectations": statistics.unsuccessful_expectations,
                "success_percent": statistics.success_percent,
            }
        }

        if evaluation_parameters is not None:
            result.update({"evaluation_parameters": evaluation_parameters})

        return result

    def get_evaluation_parameter(self, parameter_name, default_value=None):
        """Get an evaluation parameter value that has been stored in meta.

        Args:
            parameter_name (string): The name of the parameter to store.
            default_value (any): The default value to be returned if the parameter is not found.

        Returns:
            The current value of the evaluation parameter.
        """
        if "evaluation_parameters" in self._expectations_config and \
                parameter_name in self._expectations_config['evaluation_parameters']:
            return self._expectations_config['evaluation_parameters'][parameter_name]
        else:
            return default_value

    def set_evaluation_parameter(self, parameter_name, parameter_value):
        """Provide a value to be stored in the dataset evaluation_parameters object and used to evaluate
        parameterized expectations.

        Args:
            parameter_name (string): The name of the kwarg to be replaced at evaluation time
            parameter_value (any): The value to be used
        """

        if 'evaluation_parameters' not in self._expectations_config:
            self._expectations_config['evaluation_parameters'] = {}

        self._expectations_config['evaluation_parameters'].update({parameter_name: parameter_value})

    def _build_evaluation_parameters(self, expectation_args, evaluation_parameters):
        """Build a dictionary of parameters to evaluate, using the provided evaluation_paramters,
        AND mutate expectation_args by removing any parameter values passed in as temporary values during
        exploratory work.
        """

        evaluation_args = copy.deepcopy(expectation_args)

        # Iterate over arguments, and replace $PARAMETER-defined args with their
        # specified parameters.
        for key, value in evaluation_args.items():
            if isinstance(value, dict) and '$PARAMETER' in value:
                # First, check to see whether an argument was supplied at runtime
                # If it was, use that one, but remove it from the stored config
                if "$PARAMETER." + value["$PARAMETER"] in value:
                    evaluation_args[key] = evaluation_args[key]["$PARAMETER." + value["$PARAMETER"]]
                    del expectation_args[key]["$PARAMETER." + value["$PARAMETER"]]
                elif evaluation_parameters is not None and value["$PARAMETER"] in evaluation_parameters:
                    evaluation_args[key] = evaluation_parameters[value['$PARAMETER']]
                else:
                    raise KeyError("No value found for $PARAMETER " + value["$PARAMETER"])

        return evaluation_args

    ##### Output generation #####
    def _format_column_map_output(self,
        result_format, success,
        element_count, nonnull_count,
        unexpected_list, unexpected_index_list
    ):
        """Helper function to construct expectation result objects for column_map_expectations.

        Expectations support four result_formats: BOOLEAN_ONLY, BASIC, SUMMARY, and COMPLETE.
        In each case, the object returned has a different set of populated fields.
        See :ref:`result_format` for more information.

        This function handles the logic for mapping those fields for column_map_expectations.
        """

        # Retain support for string-only output formats:
        result_format = parse_result_format(result_format)

        # Incrementally add to result and return when all values for the specified level are present
        return_obj = {
            'success': success
        }

        if result_format['result_format'] == 'BOOLEAN_ONLY':
            return return_obj

        missing_count = element_count - nonnull_count
        unexpected_count = len(unexpected_list)

        if element_count > 0:
            unexpected_percent = unexpected_count / element_count
            missing_percent = missing_count / element_count

            if nonnull_count > 0:
                unexpected_percent_nonmissing = unexpected_count / nonnull_count
            else:
                unexpected_percent_nonmissing = None

        else:
            missing_percent = None
            unexpected_percent = None
            unexpected_percent_nonmissing = None

        return_obj['result'] = {
            'element_count': element_count,
            'missing_count': missing_count,
            'missing_percent': missing_percent,
            'unexpected_count': unexpected_count,
            'unexpected_percent': unexpected_percent,
            'unexpected_percent_nonmissing': unexpected_percent_nonmissing,
            'partial_unexpected_list': unexpected_list[:result_format['partial_unexpected_count']]
        }

        if result_format['result_format'] == 'BASIC':
            return return_obj

        # Try to return the most common values, if possible.
        try:
            partial_unexpected_counts = [
                {'value': key, 'count': value}
                for key, value
                in sorted(
                    Counter(unexpected_list).most_common(result_format['partial_unexpected_count']),
                    key=lambda x: (-x[1], x[0]))
            ]
        except TypeError:
            partial_unexpected_counts = ['partial_exception_counts requires a hashable type']

        return_obj['result'].update(
            {
                'partial_unexpected_index_list': unexpected_index_list[:result_format['partial_unexpected_count']] if unexpected_index_list is not None else None,
                'partial_unexpected_counts': partial_unexpected_counts
            }
        )

        if result_format['result_format'] == 'SUMMARY':
            return return_obj

        return_obj['result'].update(
            {
                'unexpected_list': unexpected_list,
                'unexpected_index_list': unexpected_index_list
            }
        )

        if result_format['result_format'] == 'COMPLETE':
            return return_obj

        raise ValueError("Unknown result_format %s." % (result_format['result_format'],))

    def _calc_map_expectation_success(self, success_count, nonnull_count, mostly):
        """Calculate success and percent_success for column_map_expectations

        Args:
            success_count (int): \
                The number of successful values in the column
            nonnull_count (int): \
                The number of nonnull values in the column
            mostly (float or None): \
                A value between 0 and 1 (or None), indicating the percentage of successes required to pass the expectation as a whole\
                If mostly=None, then all values must succeed in order for the expectation as a whole to succeed.

        Returns:
            success (boolean), percent_success (float)
        """

        if nonnull_count > 0:
            # percent_success = float(success_count)/nonnull_count
            percent_success = success_count / nonnull_count

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

            Check out :ref:`custom_expectations` for more information.
        """

        new_function = self.expectation(inspect.getargspec(function)[0][1:])(function)
        return new_function(self, *args, **kwargs)

    def test_column_map_expectation_function(self, function, *args, **kwargs):
        """Test a column map expectation function

        Args:
            function (func): The function to be tested. (Must be a valid column_map_expectation function.)
            *args          : Positional arguments to be passed the the function
            **kwargs       : Keyword arguments to be passed the the function

        Returns:
            A JSON-serializable expectation result object.

        Notes:
            This function is a thin layer to allow quick testing of new expectation functions, without having to define custom classes, etc.
            To use developed expectations from the command-line tool, you'll still need to define custom classes, etc.

            Check out :ref:`custom_expectations` for more information.
        """

        new_function = self.column_map_expectation( function )
        return new_function(self, *args, **kwargs)

    def test_column_aggregate_expectation_function(self, function, *args, **kwargs):
        """Test a column aggregate expectation function

        Args:
            function (func): The function to be tested. (Must be a valid column_aggregate_expectation function.)
            *args          : Positional arguments to be passed the the function
            **kwargs       : Keyword arguments to be passed the the function

        Returns:
            A JSON-serializable expectation result object.

        Notes:
            This function is a thin layer to allow quick testing of new expectation functions, without having to define custom classes, etc.
            To use developed expectations from the command-line tool, you'll still need to define custom classes, etc.

            Check out :ref:`custom_expectations` for more information.
        """

        new_function = self.column_aggregate_expectation( function )
        return new_function(self, *args, **kwargs)

    ##### Table shape expectations #####

    def expect_column_to_exist(
            self, column, column_index=None, result_format=None, include_config=False, 
            catch_exceptions=None, meta=None
        ):
        """Expect the specified column to exist.

        expect_column_to_exist is a :func:`expectation <great_expectations.dataset.base.Dataset.expectation>`, not a \
        `column_map_expectation` or `column_aggregate_expectation`.

        Args:
            column (str): \
                The column name.

        Other Parameters:
            column_index (int or None): \
                If not None, checks the order of the columns. The expectation will fail if the \
                column is not in location column_index (zero-indexed).
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        """

        raise NotImplementedError

    def expect_table_columns_to_match_ordered_list(self,
            column_list,
            result_format=None, include_config=False, catch_exceptions=None, meta=None
        ):
        """Expect the columns to exactly match a specified list.

        expect_table_columns_to_match_ordered_list is a :func:`expectation <great_expectations.dataset.base.DataSet.expectation>`, not a \
        `column_map_expectation` or `column_aggregate_expectation`.

        Args:
            column_list (list of str): \
                The column names, in the correct order.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        """

        raise NotImplementedError

    def expect_table_row_count_to_be_between(self,
        min_value=0,
        max_value=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the number of rows to be between two values.

        expect_table_row_count_to_be_between is a :func:`expectation <great_expectations.dataset.base.Dataset.expectation>`, \
        not a `column_map_expectation` or `column_aggregate_expectation`.

        Keyword Args:
            min_value (int or None): \
                The minimum number of rows, inclusive.
            max_value (int or None): \
                The maximum number of rows, inclusive.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable rows has no minimum.
            * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable rows has no maximum.

        See Also:
            expect_table_row_count_to_equal
        """
        raise NotImplementedError

    def expect_table_row_count_to_equal(self,
        value,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the number of rows to equal a value.

        expect_table_row_count_to_equal is a basic :func:`expectation <great_expectations.dataset.base.Dataset.expectation>`, \
        not a `column_map_expectation` or `column_aggregate_expectation`.

        Args:
            value (int): \
                The expected number of rows.

        Other Parameters:
            result_format (string or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_table_row_count_to_be_between
        """
        raise NotImplementedError

    ##### Missing values, unique values, and types #####

    def expect_column_values_to_be_unique(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect each column value to be unique.

        This expectation detects duplicates. All duplicated values are counted as exceptions.

        For example, `[1, 2, 3, 3, 3]` will return `[3, 3, 3]` in `result.exceptions_list`, with `unexpected_percent=0.6.`

        expect_column_values_to_be_unique is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
        """
        raise NotImplementedError

    def expect_column_values_to_not_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column values to not be null.

        To be counted as an exception, values must be explicitly null or missing, such as a NULL in PostgreSQL or an np.NaN in pandas.
        Empty strings don't count as null unless they have been coerced to a null type.

        expect_column_values_to_not_be_null is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_be_null

        """
        raise NotImplementedError

    def expect_column_values_to_be_null(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column values to be null.

        expect_column_values_to_be_null is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_not_be_null

        """
        raise NotImplementedError

    def expect_column_values_to_be_of_type(
        self,
        column,
        type_,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect each column entry to be a specified data type.

        expect_column_values_to_be_of_type is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            type\_ (str): \
                A string representing the data type that each column should have as entries.
                For example, "double integer" refers to an integer with double precision.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Warning:
            expect_column_values_to_be_of_type is slated for major changes in future versions of great_expectations.

            As of v0.3, great_expectations is exclusively based on pandas, which handles typing in its own peculiar way.
            Future versions of great_expectations will allow for Datasets in SQL, spark, etc.
            When we make that change, we expect some breaking changes in parts of the codebase that are based strongly on pandas notions of typing.

        See also:
            expect_column_values_to_be_in_type_list
        """
        raise NotImplementedError

    def expect_column_values_to_be_in_type_list(
        self,
        column,
        type_list,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect each column entry to match a list of specified data types.

        expect_column_values_to_be_in_type_list is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            type_list (list of str): \
                A list of strings representing the data type that each column should have as entries.
                For example, "double integer" refers to an integer with double precision.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Warning:
            expect_column_values_to_be_in_type_list is slated for major changes in future versions of great_expectations.

            As of v0.3, great_expectations is exclusively based on pandas, which handles typing in its own peculiar way.
            Future versions of great_expectations will allow for Datasets in SQL, spark, etc.
            When we make that change, we expect some breaking changes in parts of the codebase that are based strongly on pandas notions of typing.

        See also:
            expect_column_values_to_be_of_type
        """
        raise NotImplementedError

    ##### Sets and ranges #####

    def expect_column_values_to_be_in_set(self,
        column,
        values_set,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect each column value to be in a given set.

        For example:
        ::

            # my_df.my_col = [1,2,2,3,3,3]
            >>> my_df.expect_column_values_to_be_in_set(
                "my_col",
                [2,3]
            )
            {
              "success": false
              "result": {
                "unexpected_count": 1
                "unexpected_percent": 0.16666666666666666,
                "unexpected_percent_nonmissing": 0.16666666666666666,
                "partial_unexpected_list": [
                  1
                ],
              },
            }

        expect_column_values_to_be_in_set is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.


        Args:
            column (str): \
                The column name.
            values_set (set-like): \
                A set of objects used for comparison.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_not_be_in_set
        """
        raise NotImplementedError

    def expect_column_values_to_not_be_in_set(self,
        column,
        values_set,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to not be in the set.

        For example:
        ::

            # my_df.my_col = [1,2,2,3,3,3]
            >>> my_df.expect_column_values_to_be_in_set(
                "my_col",
                [1,2]
            )
            {
              "success": false
              "result": {
                "unexpected_count": 3
                "unexpected_percent": 0.5,
                "unexpected_percent_nonmissing": 0.5,
                "partial_unexpected_list": [
                  1, 2, 2
                ],
              },
            }

        expect_column_values_to_not_be_in_set is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            values_set (set-like): \
                A set of objects used for comparison.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_be_in_set
        """
        raise NotImplementedError

    def expect_column_values_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        allow_cross_type_comparisons=None,
        parse_strings_as_datetimes=None,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be between a minimum value and a maximum value (inclusive).

        expect_column_values_to_be_between is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            min_value (comparable type or None): The minimum value for a column entry.
            max_value (comparable type or None): The maximum value for a column entry.

        Keyword Args:
            allow_cross_type_comparisons (boolean or None) : If True, allow comparisons between types (e.g. integer and\
                string). Otherwise, attempting such comparisons will raise an exception.
            parse_strings_as_datetimes (boolean or None) : If True, parse min_value, max_value, and all non-null column\
                values to datetimes before making comparisons.
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable rows has no minimum.
            * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable rows has no maximum.

        See Also:
            expect_column_value_lengths_to_be_between

        """
        raise NotImplementedError

    def expect_column_values_to_be_increasing(self,
        column,
        strictly=None,
        parse_strings_as_datetimes=None,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column values to be increasing.

        By default, this expectation only works for numeric or datetime data.
        When `parse_strings_as_datetimes=True`, it can also parse strings to datetimes.

        If `strictly=True`, then this expectation is only satisfied if each consecutive value
        is strictly increasing--equal values are treated as failures.

        expect_column_values_to_be_increasing is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            strictly (Boolean or None): \
                If True, values must be strictly greater than previous values
            parse_strings_as_datetimes (boolean or None) : \
                If True, all non-null column values to datetimes before making comparisons
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_be_decreasing
        """
        raise NotImplementedError

    def expect_column_values_to_be_decreasing(self,
        column,
        strictly=None,
        parse_strings_as_datetimes=None,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column values to be decreasing.

        By default, this expectation only works for numeric or datetime data.
        When `parse_strings_as_datetimes=True`, it can also parse strings to datetimes.

        If `strictly=True`, then this expectation is only satisfied if each consecutive value
        is strictly decreasing--equal values are treated as failures.

        expect_column_values_to_be_decreasing is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            strictly (Boolean or None): \
                If True, values must be strictly greater than previous values
            parse_strings_as_datetimes (boolean or None) : \
                If True, all non-null column values to datetimes before making comparisons
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_be_increasing

        """
        raise NotImplementedError


    ##### String matching #####

    def expect_column_value_lengths_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be strings with length between a minimum value and a maximum value (inclusive).

        This expectation only works for string-type values. Invoking it on ints or floats will raise a TypeError.

        expect_column_value_lengths_to_be_between is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            min_value (int or None): \
                The minimum value for a column entry length.
            max_value (int or None): \
                The maximum value for a column entry length.
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable rows has no minimum.
            * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable rows has no maximum.

        See Also:
            expect_column_value_lengths_to_equal
        """
        raise NotImplementedError

    def expect_column_value_lengths_to_equal(self,
        column,
        value,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be strings with length equal to the provided value.

        This expectation only works for string-type values. Invoking it on ints or floats will raise a TypeError.

        expect_column_values_to_be_between is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            value (int or None): \
                The expected value for a column entry length.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_value_lengths_to_be_between
        """

    def expect_column_values_to_match_regex(self,
        column,
        regex,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be strings that match a given regular expression. Valid matches can be found \
        anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat", \
        "aa", "a", and "t", and the following strings as unexpected: "fish", "dog".

        expect_column_values_to_match_regex is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            regex (str): \
                The regular expression the column entries should match.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_not_match_regex
            expect_column_values_to_match_regex_list
        """
        raise NotImplementedError

    def expect_column_values_to_not_match_regex(self,
        column,
        regex,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be strings that do NOT match a given regular expression. The regex must not match \
        any portion of the provided string. For example, "[at]+" would identify the following strings as expected: \
        "fish", "dog", and the following as unexpected: "cat", "hat".

        expect_column_values_to_not_match_regex is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            regex (str): \
                The regular expression the column entries should NOT match.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_match_regex
            expect_column_values_to_match_regex_list
        """
        raise NotImplementedError

    def expect_column_values_to_match_regex_list(self,
        column,
        regex_list,
        match_on="any",
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the column entries to be strings that can be matched to either any of or all of a list of regular expressions.
        Matches can be anywhere in the string.

        expect_column_values_to_match_regex_list is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            regex_list (list): \
                The list of regular expressions which the column entries should match

        Keyword Args:
            match_on= (string): \
                "any" or "all".
                Use "any" if the value should match at least one regular expression in the list.
                Use "all" if it should match each regular expression in the list.
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_match_regex
            expect_column_values_to_not_match_regex
        """
        raise NotImplementedError

    def expect_column_values_to_not_match_regex_list(self, column, regex_list,
                                                mostly=None,
                                                result_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the column entries to be strings that do not match any of a list of regular expressions. Matches can \
        be anywhere in the string.


        expect_column_values_to_not_match_regex_list is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            regex_list (list): \
                The list of regular expressions which the column entries should not match

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_match_regex_list
        """
        raise NotImplementedError

    ##### Datetime and JSON parsing #####

    def expect_column_values_to_match_strftime_format(self,
        column,
        strftime_format,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be strings representing a date or time with a given format.

        expect_column_values_to_match_strftime_format is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.
            strftime_format (str): \
                A strftime format string to use for matching

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        """
        raise NotImplementedError

    def expect_column_values_to_be_dateutil_parseable(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be parseable using dateutil.

        expect_column_values_to_be_dateutil_parseable is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
        """
        raise NotImplementedError

    def expect_column_values_to_be_json_parseable(self,
        column,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be data written in JavaScript Object Notation.

        expect_column_values_to_be_json_parseable is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_match_json_schema
        """
        raise NotImplementedError

    def expect_column_values_to_match_json_schema(self,
        column,
        json_schema,
        mostly=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column entries to be JSON objects matching a given JSON schema.

        expect_column_values_to_match_json_schema is a :func:`column_map_expectation <great_expectations.dataset.base.Dataset.column_map_expectation>`.

        Args:
            column (str): \
                The column name.

        Keyword Args:
            mostly (None or a float between 0 and 1): \
                Return `"success": True` if at least mostly percent of values match the expectation. \
                For more detail, see :ref:`mostly`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        See Also:
            expect_column_values_to_be_json_parseable

            The JSON-schema docs at: http://json-schema.org/
        """
        raise NotImplementedError

    ##### Aggregate functions #####

    def expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(self,
                                                                                    column, distribution,
                                                                                    p_value=0.05, params=None,
                                                                                    result_format=None,
                                                                                    include_config=False,
                                                                                    catch_exceptions=None, meta=None):
        """
        Expect the column values to be distributed similarly to a scipy distribution. \

        This expectation compares the provided column to the specified continuous distribution with a parameteric \
        Kolmogorov-Smirnov test. The K-S test compares the provided column to the cumulative density function (CDF) of \
        the specified scipy distribution. If you don't know the desired distribution shape parameters, use the \
        `ge.dataset.util.infer_distribution_parameters()` utility function to estimate them.

        It returns 'success'=True if the p-value from the K-S test is greater than or equal to the provided p-value.

        expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than is a \
        :func:`column_aggregate_expectation <great_expectations.dataset.base.DataSet.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            distribution (str): \
                The scipy distribution name. See: https://docs.scipy.org/doc/scipy/reference/stats.html
            p_value (float): \
                The threshold p-value for a passing test. Default is 0.05.
            params (dict or list) : \
                A dictionary or positional list of shape parameters that describe the distribution you want to test the\
                data against. Include key values specific to the distribution from the appropriate scipy \
                distribution CDF function. 'loc' and 'scale' are used as translational parameters.\
                See https://docs.scipy.org/doc/scipy/reference/stats.html#continuous-distributions

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "details":
                        "expected_params" (dict): The specified or inferred parameters of the distribution to test against
                        "ks_results" (dict): The raw result of stats.kstest()
                }

            * The Kolmogorov-Smirnov test's null hypothesis is that the column is similar to the provided distribution.
            * Supported scipy distributions:
                -norm
                -beta
                -gamma
                -uniform
                -chi2
                -expon

        """
        raise NotImplementedError

    def expect_column_mean_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the column mean to be between a minimum value and a maximum value (inclusive).

        expect_column_mean_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            min_value (float or None): \
                The minimum value for the column mean.
            max_value (float or None): \
                The maximum value for the column mean.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (float) The true mean for the column
                }

            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound.
            * If max_value is None, then min_value is treated as a lower bound.

        See Also:
            expect_column_median_to_be_between
            expect_column_stdev_to_be_between
        """
        raise NotImplementedError

    def expect_column_median_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the column median to be between a minimum value and a maximum value.

        expect_column_median_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            min_value (int or None): \
                The minimum value for the column median.
            max_value (int or None): \
                The maximum value for the column median.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (float) The true median for the column
                }

            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound
            * If max_value is None, then min_value is treated as a lower bound

        See Also:
            expect_column_mean_to_be_between
            expect_column_stdev_to_be_between

        """
        raise NotImplementedError

    def expect_column_stdev_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the column standard deviation to be between a minimum value and a maximum value.

        expect_column_stdev_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            min_value (float or None): \
                The minimum value for the column standard deviation.
            max_value (float or None): \
                The maximum value for the column standard deviation.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (float) The true standard deviation for the column
                }

            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound
            * If max_value is None, then min_value is treated as a lower bound

        See Also:
            expect_column_mean_to_be_between
            expect_column_median_to_be_between
        """
        raise NotImplementedError

    def expect_column_unique_value_count_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the number of unique values to be between a minimum value and a maximum value.

        expect_column_unique_value_count_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            min_value (int or None): \
                The minimum number of unique values allowed.
            max_value (int or None): \
                The maximum number of unique values allowed.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (int) The number of unique values in the column
                }

            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound
            * If max_value is None, then min_value is treated as a lower bound

        See Also:
            expect_column_proportion_of_unique_values_to_be_between
        """
        raise NotImplementedError

    def expect_column_proportion_of_unique_values_to_be_between(self,
        column,
        min_value=0,
        max_value=1,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the proportion of unique values to be between a minimum value and a maximum value.

        For example, in a column containing [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], there are 4 unique values and 10 total \
        values for a proportion of 0.4.

        Args:
            column (str): \
                The column name.
            min_value (float or None): \
                The minimum proportion of unique values. (Proportions are on the range 0 to 1)
            max_value (float or None): \
                The maximum proportion of unique values. (Proportions are on the range 0 to 1)

        expect_column_unique_value_count_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (float) The proportion of unique values in the column
                }

            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound
            * If max_value is None, then min_value is treated as a lower bound

        See Also:
            expect_column_unique_value_count_to_be_between
        """
        raise NotImplementedError

    def expect_column_most_common_value_to_be_in_set(self,
        column,
        value_set,
        ties_okay=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the most common value to be within the designated value set

        expect_column_most_common_value_to_be_in_set is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name
            value_set (set-like): \
                A list of potential values to match

        Keyword Args:
            ties_okay (boolean or None): \
                If True, then the expectation will still succeed if values outside the designated set are as common (but not more common) than designated values

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (list) The most common values in the column
                }

            `observed_value` contains a list of the most common values.
            Often, this will just be a single element. But if there's a tie for most common among multiple values,
            `observed_value` will contain a single copy of each most common value.

        """
        raise NotImplementedError

    def expect_column_sum_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the column to sum to be between an min and max value

        expect_column_sum_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name
            min_value (comparable type or None): \
                The minimum number of unique values allowed.
            max_value (comparable type or None): \
                The maximum number of unique values allowed.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (list) The actual column sum
                }


            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound
            * If max_value is None, then min_value is treated as a lower bound

        """
        raise NotImplementedError

    def expect_column_min_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the column to sum to be between an min and max value

        expect_column_min_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name
            min_value (comparable type or None): \
                The minimum number of unique values allowed.
            max_value (comparable type or None): \
                The maximum number of unique values allowed.

        Keyword Args:
            parse_strings_as_datetimes (Boolean or None): \
                If True, parse min_value, max_values, and all non-null column values to datetimes before making comparisons.
            output_strftime_format (str or None): \
                A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (list) The actual column min
                }


            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound
            * If max_value is None, then min_value is treated as a lower bound

        """
        raise NotImplementedError

    def expect_column_max_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect the column max to be between an min and max value

        expect_column_sum_to_be_between is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name
            min_value (comparable type or None): \
                The minimum number of unique values allowed.
            max_value (comparable type or None): \
                The maximum number of unique values allowed.

        Keyword Args:
            parse_strings_as_datetimes (Boolean or None): \
                If True, parse min_value, max_values, and all non-null column values to datetimes before making comparisons.
            output_strftime_format (str or None): \
                A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (list) The actual column max
                }


            * min_value and max_value are both inclusive.
            * If min_value is None, then max_value is treated as an upper bound
            * If max_value is None, then min_value is treated as a lower bound

        """
        raise NotImplementedError

    ### Distributional expectations
    def expect_column_chisquare_test_p_value_to_be_greater_than(self,
        column,
        partition_object=None,
        p=0.05,
        tail_weight_holdout=0,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column values to be distributed similarly to the provided categorical partition. \

        This expectation compares categorical distributions using a Chi-squared test. \
        It returns `success=True` if values in the column match the distribution of the provided partition.

        expect_column_chisquare_test_p_value_to_be_greater_than is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            partition_object (dict): \
                The expected partition object (see :ref:`partition_object`).
            p (float): \
                The p-value threshold for rejecting the null hypothesis of the Chi-Squared test.\
                For values below the specified threshold, the expectation will return `success=False`,\
                rejecting the null hypothesis that the distributions are the same.\
                Defaults to 0.05.

        Keyword Args:
            tail_weight_holdout (float between 0 and 1 or None): \
                The amount of weight to split uniformly between values observed in the data but not present in the \
                provided partition. tail_weight_holdout provides a mechanism to make the test less strict by \
                assigning positive weights to unknown values observed in the data that are not present in the \
                partition.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (float) The true p-value of the Chi-squared test
                    "details": {
                        "observed_partition" (dict):
                            The partition observed in the data.
                        "expected_partition" (dict):
                            The partition expected from the data, after including tail_weight_holdout
                    }
                }

        """
        raise NotImplementedError

    def expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(self,
        column,
        partition_object=None,
        p=0.05,
        bootstrap_samples=None,
        bootstrap_sample_size=None,
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """Expect column values to be distributed similarly to the provided continuous partition. This expectation \
        compares continuous distributions using a bootstrapped Kolmogorov-Smirnov test. It returns `success=True` if \
        values in the column match the distribution of the provided partition.

        The expected cumulative density function (CDF) is constructed as a linear interpolation between the bins, \
        using the provided weights. Consequently the test expects a piecewise uniform distribution using the bins from \
        the provided partition object.

        expect_column_bootstrapped_ks_test_p_value_to_be_greater_than is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            partition_object (dict): \
                The expected partition object (see :ref:`partition_object`).
            p (float): \
                The p-value threshold for the Kolmogorov-Smirnov test.
                For values below the specified threshold the expectation will return `success=False`, rejecting the \
                null hypothesis that the distributions are the same. \
                Defaults to 0.05.

        Keyword Args:
            bootstrap_samples (int): \
                The number bootstrap rounds. Defaults to 1000.
            bootstrap_sample_size (int): \
                The number of samples to take from the column for each bootstrap. A larger sample will increase the \
                specificity of the test. Defaults to 2 * len(partition_object['weights'])

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                    "observed_value": (float) The true p-value of the KS test
                    "details": {
                        "bootstrap_samples": The number of bootstrap rounds used
                        "bootstrap_sample_size": The number of samples taken from
                            the column in each bootstrap round
                        "observed_cdf": The cumulative density function observed
                            in the data, a dict containing 'x' values and cdf_values
                            (suitable for plotting)
                        "expected_cdf" (dict):
                            The cumulative density function expected based on the
                            partition object, a dict containing 'x' values and
                            cdf_values (suitable for plotting)
                        "observed_partition" (dict):
                            The partition observed on the data, using the provided
                            bins but also expanding from min(column) to max(column)
                        "expected_partition" (dict):
                            The partition expected from the data. For KS test,
                            this will always be the partition_object parameter
                    }
                }

        """
        raise NotImplementedError

    def expect_column_kl_divergence_to_be_less_than(self,
        column,
        partition_object=None,
        threshold=None,
        tail_weight_holdout=0,
        internal_weight_holdout=0,
        result_format=None, include_config=False, catch_exceptions=None, meta=None):
        """Expect the Kulback-Leibler (KL) divergence (relative entropy) of the specified column with respect to the \
        partition object to be lower than the provided threshold.

        KL divergence compares two distributions. The higher the divergence value (relative entropy), the larger the \
        difference between the two distributions. A relative entropy of zero indicates that the data are \
        distributed identically, `when binned according to the provided partition`.

        In many practical contexts, choosing a value between 0.5 and 1 will provide a useful test.

        This expectation works on both categorical and continuous partitions. See notes below for details.

        expect_column_kl_divergence_to_be_less_than is a :func:`column_aggregate_expectation <great_expectations.dataset.base.Dataset.column_aggregate_expectation>`.

        Args:
            column (str): \
                The column name.
            partition_object (dict): \
                The expected partition object (see :ref:`partition_object`).
            threshold (float): \
                The maximum KL divergence to for which to return `success=True`. If KL divergence is larger than the\
                provided threshold, the test will return `success=False`.

        Keyword Args:
            internal_weight_holdout (float between 0 and 1 or None): \
                The amount of weight to split uniformly among zero-weighted partition bins. internal_weight_holdout \
                provides a mechanims to make the test less strict by assigning positive weights to values observed in \
                the data for which the partition explicitly expected zero weight. With no internal_weight_holdout, \
                any value observed in such a region will cause KL divergence to rise to +Infinity.\
                Defaults to 0.
            tail_weight_holdout (float between 0 and 1 or None): \
                The amount of weight to add to the tails of the histogram. Tail weight holdout is split evenly between\
                (-Infinity, min(partition_object['bins'])) and (max(partition_object['bins']), +Infinity). \
                tail_weight_holdout provides a mechanism to make the test less strict by assigning positive weights to \
                values observed in the data that are not present in the partition. With no tail_weight_holdout, \
                any value observed outside the provided partition_object will cause KL divergence to rise to +Infinity.\
                Defaults to 0.

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        Notes:
            These fields in the result object are customized for this expectation:
            ::

                {
                  "observed_value": (float) The true KL divergence (relative entropy)
                  "details": {
                    "observed_partition": (dict) The partition observed in the data
                    "expected_partition": (dict) The partition against which the data were compared,
                                            after applying specified weight holdouts.
                  }
                }

            If the partition_object is categorical, this expectation will expect the values in column to also be \
            categorical.

                * If the column includes values that are not present in the partition, the tail_weight_holdout will be \
                equally split among those values, providing a mechanism to weaken the strictness of the expectation \
                (otherwise, relative entropy would immediately go to infinity).
                * If the partition includes values that are not present in the column, the test will simply include \
                zero weight for that value.

            If the partition_object is continuous, this expectation will discretize the values in the column according \
            to the bins specified in the partition_object, and apply the test to the resulting distribution.

                * The internal_weight_holdout and tail_weight_holdout parameters provide a mechanism to weaken the \
                expectation, since an expected weight of zero would drive relative entropy to be infinite if any data \
                are observed in that interval.
                * If internal_weight_holdout is specified, that value will be distributed equally among any intervals \
                with weight zero in the partition_object.
                * If tail_weight_holdout is specified, that value will be appended to the tails of the bins \
                ((-Infinity, min(bins)) and (max(bins), Infinity).

        See also:
            expect_column_chisquare_test_p_value_to_be_greater_than
            expect_column_bootstrapped_ks_test_p_value_to_be_greater_than

        """
        raise NotImplementedError

    ### Column pairs ###

    def expect_column_pair_values_to_be_equal(self,
        column_A,
        column_B,
        ignore_row_if="both_values_are_missing",
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """
        Expect the values in column A to be the same as column B.

        Args:
            column_A (str): The first column name
            column_B (str): The second column name

        Keyword Args:
            ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        """
        raise NotImplementedError

    def expect_column_pair_values_A_to_be_greater_than_B(self,
        column_A,
        column_B,
        or_equal=None,
        parse_strings_as_datetimes=None,
        allow_cross_type_comparisons=None,
        ignore_row_if="both_values_are_missing",
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """
        Expect values in column A to be greater than column B.

        Args:
            column_A (str): The first column name
            column_B (str): The second column name
            or_equal (boolean or None): If True, then values can be equal, not strictly greater

        Keyword Args:
            allow_cross_type_comparisons (boolean or None) : If True, allow comparisons between types (e.g. integer and\
                string). Otherwise, attempting such comparisons will raise an exception.

        Keyword Args:
            ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        """
        raise NotImplementedError


    def expect_column_pair_values_to_be_in_set(self,
        column_A,
        column_B,
        value_pairs_set,
        ignore_row_if="both_values_are_missing",
        result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """
        Expect paired values from columns A and B to belong to a set of valid pairs.

        Args:
            column_A (str): The first column name
            column_B (str): The second column name
            value_pairs_set (list of tuples): All the valid pairs to be matched

        Keyword Args:
            ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither

        Other Parameters:
            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result object. \
                For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
                For more detail, see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

        """
        raise NotImplementedError


ValidationStatistics = namedtuple("ValidationStatistics", [
    "evaluated_expectations",
    "successful_expectations",
    "unsuccessful_expectations",
    "success_percent",
    "success",
])


def _calc_validation_statistics(validation_results):
    """
    Calculate summary statistics for the validation results and
    return ``ExpectationStatistics``.
    """
    # calc stats
    successful_expectations = sum(exp["success"] for exp in validation_results)
    evaluated_expectations = len(validation_results)
    unsuccessful_expectations = evaluated_expectations - successful_expectations
    success = successful_expectations == evaluated_expectations
    try:
        success_percent = successful_expectations / evaluated_expectations * 100
    except ZeroDivisionError:
        success_percent = float("nan")

    return ValidationStatistics(
        successful_expectations=successful_expectations,
        evaluated_expectations=evaluated_expectations,
        unsuccessful_expectations=unsuccessful_expectations,
        success=success,
        success_percent=success_percent,
    )
