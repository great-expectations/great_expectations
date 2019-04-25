from __future__ import division
import json
import inspect
import copy
from functools import wraps
import traceback
import warnings
import logging
from six import PY3, string_types
from collections import namedtuple

from collections import (
    Counter,
    defaultdict
)

from great_expectations.version import __version__
from great_expectations.data_asset.util import DotDict, recursively_convert_to_json_serializable, parse_result_format
from great_expectations.dataset.autoinspect import columns_exist

logger = logging.getLogger("DataAsset")

class DataAsset(object):

    def __init__(self, *args, **kwargs):
        """
        Initialize the DataAsset.

        :param autoinspect_func (function) = None: The autoinspection function that should be run on the data_asset to
            establish baseline expectations.

        Note: DataAsset is designed to support multiple inheritance (e.g. PandasDataset inherits from both a
        Pandas DataFrame and Dataset which inherits from DataAsset), so it accepts generic *args and **kwargs arguments so that they can also be
        passed to other parent classes. In python 2, there isn't a clean way to include all of *args, **kwargs, and a
        named kwarg...so we use the inelegant solution of popping from kwargs, leaving the support for the autoinspect_func
        parameter not obvious from the signature.

        """
        autoinspect_func = kwargs.pop("autoinspect_func", None)
        initial_config = kwargs.pop("config", None)
        data_asset_name = kwargs.pop("data_asset_name", None)

        super(DataAsset, self).__init__(*args, **kwargs)
        self._initialize_expectations(config=initial_config, data_asset_name=data_asset_name)
        if autoinspect_func is not None:
            autoinspect_func(self)

    def autoinspect(self, autoinspect_func=columns_exist):
        autoinspect_func(self)

    @classmethod
    def expectation(cls, method_arg_names):
        """Manages configuration and running of expectation objects.

        Expectation builds and saves a new expectation configuration to the DataAsset object. It is the core decorator \
        used by great expectations to manage expectation configurations.

        Args:
            method_arg_names (List) : An ordered list of the arguments used by the method implementing the expectation \
                (typically the result of inspection). Positional arguments are explicitly mapped to \
                keyword arguments when the expectation is run.

        Notes:
            Intermediate decorators that call the core @expectation decorator will most likely need to pass their \
            decorated methods' signature up to the expectation decorator. For example, the MetaPandasDataset \
            column_map_expectation decorator relies on the DataAsset expectation decorator, but will pass through the \
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

                # Get the name of the method
                method_name = func.__name__

                # Combine all arguments into a single new "kwargs"
                all_args = dict(zip(method_arg_names, args))
                all_args.update(kwargs)

                # Unpack display parameters; remove them from all_args if appropriate
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

                # Get the signature of the inner wrapper:
                if PY3:
                    argspec = inspect.getfullargspec(func)[0][1:]
                else:
                    argspec = inspect.getargspec(func)[0][1:]

                if "result_format" in argspec:
                    all_args["result_format"] = result_format
                else:
                    if "result_format" in all_args:
                        del all_args["result_format"]

                all_args = recursively_convert_to_json_serializable(all_args)

                # Patch in PARAMETER args, and remove locally-supplied arguments
                # This will become the stored config
                expectation_args = copy.deepcopy(all_args)

                if "evaluation_parameters" in self._expectations_config:
                    evaluation_args = self._build_evaluation_parameters(expectation_args,
                                                                        self._expectations_config["evaluation_parameters"])  # This will be passed to the evaluation
                else:
                    evaluation_args = self._build_evaluation_parameters(
                        expectation_args, None)

                # Construct the expectation_config object
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
                    return_obj["expectation_config"] = copy.deepcopy(
                        expectation_config)

                if catch_exceptions:
                    return_obj["exception_info"] = {
                        "raised_exception": raised_exception,
                        "exception_message": exception_message,
                        "exception_traceback": exception_traceback
                    }

                # Add a "success" object to the config
                expectation_config["success_on_last_run"] = return_obj["success"]

                # Add meta to return object
                if meta is not None:
                    return_obj['meta'] = meta


                return_obj = recursively_convert_to_json_serializable(
                    return_obj)
                return return_obj

            return wrapper

        return outer_wrapper

    def _initialize_expectations(self, config=None, data_asset_name=None):
        """Instantiates `_expectations_config` as empty by default or with a specified expectation `config`.
        In addition, this always sets the `default_expectation_args` to:
            `include_config`: False,
            `catch_exceptions`: False,
            `output_format`: 'BASIC'

        By default, initializes data_asset_type to the name of the implementing class, but subclasses
        that have interoperable semantics (e.g. Dataset) may override that parameter to clarify their
        interoperability.

        Args:
            config (json): \
                A json-serializable expectation config. \
                If None, creates default `_expectations_config` with an empty list of expectations and \
                key value `data_asset_name` as `data_asset_name`.

            data_asset_name (string): \
                The name to assign to `_expectations_config.data_asset_name` if `config` is not provided.

        """
        if config != None:
            #!!! Should validate the incoming config with jsonschema here

            # Copy the original so that we don't overwrite it by accident
            # Pandas incorrectly interprets this as an attempt to create a column and throws up a warning. Suppress it
            # since we are subclassing.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                self._expectations_config = DotDict(copy.deepcopy(config))
                if data_asset_name is not None:
                    self._expectations_config["data_asset_name"] = data_asset_name

        else:
            # Pandas incorrectly interprets this as an attempt to create a column and throws up a warning. Suppress it
            # since we are subclassing.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                self._expectations_config = DotDict({
                    "data_asset_name": data_asset_name,
                    "data_asset_type": self.__class__.__name__,
                    "meta": {
                        "great_expectations.__version__": __version__
                    },
                    "expectations": []
                })

        # Pandas incorrectly interprets this as an attempt to create a column and throws up a warning. Suppress it
        # since we are subclassing.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            self.default_expectation_args = {
                "include_config": False,
                "catch_exceptions": False,
                "result_format": 'BASIC',
            }

    def _append_expectation(self, expectation_config):
        """Appends an expectation to `DataAsset._expectations_config` and drops existing expectations of the same type.

           If `expectation_config` is a column expectation, this drops existing expectations that are specific to \
           that column and only if it is the same expectation type as `expectation_config`. Otherwise, if it's not a \
           column expectation, this drops existing expectations of the same type as `expectation config`. \
           After expectations of the same type are dropped, `expectation_config` is appended to `DataAsset._expectations_config`.

           Args:
               expectation_config (json): \
                   The JSON-serializable expectation to be added to the DataAsset expectations in `_expectations_config`.

           Notes:
               May raise future errors once json-serializable tests are implemented to check for correct arg formatting

        """
        expectation_type = expectation_config['expectation_type']

        # Test to ensure the new expectation is serializable.
        # FIXME: If it's not, are we sure we want to raise an error?
        # FIXME: Should we allow users to override the error?
        # FIXME: Should we try to convert the object using something like recursively_convert_to_json_serializable?
        json.dumps(expectation_config)

        # Drop existing expectations with the same expectation_type.
        # For column_expectations, _append_expectation should only replace expectations
        # where the expectation_type AND the column match
        #!!! This is good default behavior, but
        #!!!    it needs to be documented, and
        #!!!    we need to provide syntax to override it.

        if 'column' in expectation_config['kwargs']:
            column = expectation_config['kwargs']['column']

            self._expectations_config.expectations = [f for f in filter(
                lambda exp: (exp['expectation_type'] != expectation_type) or (
                    'column' in exp['kwargs'] and exp['kwargs']['column'] != column),
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
        """Copies and cleans all expectations provided by their index in DataAsset._expectations_config.expectations.

           Applies the _copy_and_clean_up_expectation method to multiple expectations, provided by their index in \
           `DataAsset,_expectations_config.expectations`. Returns a list of the copied and cleaned expectations.

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
            raise ValueError("Conflicting column names in remove_expectation: %s and %s" % (
                column, expectation_kwargs["column"]))

        if column != None:
            expectation_kwargs["column"] = column

        match_indexes = []
        for i, exp in enumerate(self._expectations_config.expectations):
            if expectation_type == None or (expectation_type == exp['expectation_type']):
                # if column == None or ('column' not in exp['kwargs']) or (exp['kwargs']['column'] == column) or (exp['kwargs']['column']==:
                match = True

                for k, v in expectation_kwargs.items():
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
                raise ValueError(
                    'Multiple expectations matched arguments. No expectations removed.')
            else:

                if not dry_run:
                    self._expectations_config.expectations = [i for j, i in enumerate(
                        self._expectations_config.expectations) if j not in match_indexes]
                else:
                    return self._copy_and_clean_up_expectations_from_indexes(match_indexes)

        else:  # Exactly one match
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
            warnings.warn(
                "Removed %s expectations that were 'False'" % len(res))

    def get_default_expectation_arguments(self):
        """Fetch default expectation arguments for this data_asset

        Returns:
            A dictionary containing all the current default expectation arguments for a data_asset

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
        """Set a default expectation argument for this data_asset

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
            discard_failed_expectations (boolean): \
                Only include expectations with success_on_last_run=True in the exported config.  Defaults to `True`.
            discard_result_format_kwargs (boolean): \
                In returned expectation objects, suppress the `result_format` parameter. Defaults to `True`.
            discard_include_configs_kwargs (boolean): \
                In returned expectation objects, suppress the `include_configs` parameter. Defaults to `True`.
            discard_catch_exceptions_kwargs (boolean): \
                In returned expectation objects, suppress the `catch_exceptions` parameter.  Defaults to `True`.

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
                # Note: This is conservative logic.
                # Instead of retaining expectations IFF success==True, it discard expectations IFF success==False.
                # In cases where expectation["success"] is missing or None, expectations are *retained*.
                # Such a case could occur if expectations were loaded from a config file and never run.
                if "success_on_last_run" in expectation and expectation["success_on_last_run"] == False:
                    discards["failed_expectations"] += 1
                else:
                    new_expectations.append(expectation)

            expectations = new_expectations

        for expectation in expectations:
            # FIXME: Factor this out into a new function. The logic is duplicated in remove_expectation, which calls _copy_and_clean_up_expectation
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
                print("WARNING: get_expectations_config discarded")
                if discard_failed_expectations:
                    print("\t%d failing expectations" %
                          discards["failed_expectations"])
                if discard_result_format_kwargs:
                    print("\t%d result_format kwargs" %
                          discards["result_format"])
                if discard_include_configs_kwargs:
                    print("\t%d include_configs kwargs" %
                          discards["include_configs"])
                if discard_catch_exceptions_kwargs:
                    print("\t%d catch_exceptions kwargs" %
                          discards["catch_exceptions"])
                print("If you wish to change this behavior, please set discard_failed_expectations, discard_result_format_kwargs, discard_include_configs_kwargs, and discard_catch_exceptions_kwargs appropirately.")

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

           Writes the DataAsset's expectation config to the specified JSON ``filepath``. Failing expectations \
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
        if filepath == None:
            # FIXME: Fetch the proper filepath from the project config
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

            Use the default expectations_config=None to validate the expectations config associated with the DataAsset.

            Args:
                expectations_config (json or None): \
                    If None, uses the expectations config generated with the DataAsset during the current session. \
                    If a JSON file, validates those expectations.
                evaluation_parameters (dict or None): \
                    If None, uses the evaluation_paramters from the expectations_config provided or as part of the data_asset.
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
                    If True, expectation results are only returned when ``success = False`` \

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
                warnings.warn(
                    "WARNING: This configuration object was built using version %s of great_expectations, but is currently being valided by version %s." % (expectations_config['meta']['great_expectations.__version__'], __version__))
        except KeyError:
            warnings.warn(
                "WARNING: No great_expectations version found in configuration object.")

        for expectation in expectations_config['expectations']:

            try:
                expectation_method = getattr(
                    self, expectation['expectation_type'])

                if result_format is not None:
                    expectation['kwargs'].update({'result_format': result_format})

                # Counting the number of unexpected values can be expensive when there is a large
                # number of np.nan values.
                # This only happens on expect_column_values_to_not_be_null expectations.
                # Since there is no reason to look for most common unexpected values in this case,
                # we will instruct the result formatting method to skip this step.

                if expectation['expectation_type'] in ['expect_column_values_to_not_be_null',
                                                       'expect_column_values_to_be_null']:
                    expectation['kwargs']['result_format'] = parse_result_format(expectation['kwargs']['result_format'])
                    expectation['kwargs']['result_format']['partial_unexpected_count'] = 0

                # A missing parameter should raise a KeyError
                evaluation_args = self._build_evaluation_parameters(
                    expectation['kwargs'], evaluation_parameters)

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

            # if include_config:
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
                if exp["success"] == False:
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
        """Provide a value to be stored in the data_asset evaluation_parameters object and used to evaluate
        parameterized expectations.

        Args:
            parameter_name (string): The name of the kwarg to be replaced at evaluation time
            parameter_value (any): The value to be used
        """

        if 'evaluation_parameters' not in self._expectations_config:
            self._expectations_config['evaluation_parameters'] = {}

        self._expectations_config['evaluation_parameters'].update(
            {parameter_name: parameter_value})

    def set_data_asset_name(self, data_asset_name):
        """Sets the name of this data_asset as stored in the expectations configuration."""
        self._expectations_config['data_asset_name'] = data_asset_name

    def get_data_asset_name(self):
        """Gets the current name of this data_asset as stored in the expectations configuration."""
        return self._expectations_config['data_asset_name']

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
                    evaluation_args[key] = evaluation_args[key]["$PARAMETER." +
                                                                value["$PARAMETER"]]
                    del expectation_args[key]["$PARAMETER." +
                                              value["$PARAMETER"]]
                elif evaluation_parameters is not None and value["$PARAMETER"] in evaluation_parameters:
                    evaluation_args[key] = evaluation_parameters[value['$PARAMETER']]
                else:
                    raise KeyError(
                        "No value found for $PARAMETER " + value["$PARAMETER"])

        return evaluation_args

    ##### Output generation #####
    def _format_map_output(self,
                                  result_format, success,
                                  element_count, nonnull_count,
                                  unexpected_count,
                                  unexpected_list, unexpected_index_list
                                  ):
        """Helper function to construct expectation result objects for map_expectations (such as column_map_expectation
        and file_lines_map_expectation).

        Expectations support four result_formats: BOOLEAN_ONLY, BASIC, SUMMARY, and COMPLETE.
        In each case, the object returned has a different set of populated fields.
        See :ref:`result_format` for more information.

        This function handles the logic for mapping those fields for column_map_expectations.
        """
        # NB: unexpected_count parameter is explicit some implementing classes may limit the length of unexpected_list

        # Retain support for string-only output formats:
        result_format = parse_result_format(result_format)

        # Incrementally add to result and return when all values for the specified level are present
        return_obj = {
            'success': success
        }

        if result_format['result_format'] == 'BOOLEAN_ONLY':
            return return_obj

        missing_count = element_count - nonnull_count

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
        if 0 < result_format.get('partial_unexpected_count'):
            try:
                partial_unexpected_counts = [
                {'value': key, 'count': value}
                for key, value
                in sorted(
                    Counter(unexpected_list).most_common(result_format['partial_unexpected_count']),
                    key=lambda x: (-x[1], x[0]))
                ]
            except TypeError:
                partial_unexpected_counts = [
                    'partial_exception_counts requires a hashable type']
            finally:
                return_obj['result'].update(
                    {
                        'partial_unexpected_index_list': unexpected_index_list[:result_format[
                            'partial_unexpected_count']] if unexpected_index_list is not None else None,
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

        raise ValueError("Unknown result_format %s." %
                         (result_format['result_format'],))

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

            if mostly != None:
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

        if PY3:
            argspec = inspect.getfullargspec(function)[0][1:]
        else:
            argspec = inspect.getargspec(function)[0][1:]

        new_function = self.expectation(argspec)(function)
        return new_function(self, *args, **kwargs)

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
