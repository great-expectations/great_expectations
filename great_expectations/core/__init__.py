import logging
import json
from abc import ABC, abstractmethod
from copy import deepcopy, copy

from six import string_types

from marshmallow import Schema, fields, ValidationError, pre_load, post_load, pre_dump

from great_expectations import __version__ as ge_version
from great_expectations.types import DictDot

from great_expectations.exceptions import InvalidExpectationConfigurationError, InvalidExpectationKwargsError, \
    InvalidDataContextKeyError

logger = logging.getLogger(__name__)

RESULT_FORMATS = [
    "BOOLEAN_ONLY",
    "BASIC",
    "COMPLETE",
    "SUMMARY"
]


def convert_to_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable

    Args:
        data: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    import numpy as np
    import pandas as pd
    from six import string_types, integer_types
    import datetime
    import decimal
    import sys

    # If it's one of our types, we use our own conversion; this can move to full schema (see DataAssetIdentifier below)
    # once nesting goes all the way down
    if isinstance(data, (ExpectationConfiguration, ExpectationSuite, ExpectationValidationResult,
                         ExpectationSuiteValidationResult)):
        return data.to_json_dict()

    if isinstance(data, DataAssetIdentifier):
        return dataAssetIdentifierSchema.dump(data).data

    try:
        if not isinstance(data, list) and np.isnan(data):
            # np.isnan is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return None
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, (string_types, integer_types, float, bool)):
        # No problem to encode json
        return data

    elif isinstance(data, dict):
        new_dict = {}
        for key in data:
            # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string
            new_dict[str(key)] = convert_to_json_serializable(data[key])

        return new_dict

    elif isinstance(data, (list, tuple, set)):
        new_list = []
        for val in data:
            new_list.append(convert_to_json_serializable(val))

        return new_list

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        return [convert_to_json_serializable(x) for x in data.tolist()]

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return data

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return bool(data)

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return int(data)

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(data, sys.float_info.dig))

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        return [{
            index_name: convert_to_json_serializable(idx),
            value_name: convert_to_json_serializable(val)
        } for idx, val in data.iteritems()]

    elif isinstance(data, pd.DataFrame):
        return convert_to_json_serializable(data.to_dict(orient='records'))

    elif isinstance(data, decimal.Decimal):
        # FIXME: warn about lossy conversion here only when appropriate
        logger.warning("Converting decimal %d to float object to support serialization. This may be lossy." % data)
        return float(data)

    else:
        raise TypeError('%s is of type %s which cannot be serialized.' % (
            str(data), type(data).__name__))


def ensure_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable

    Args:
        data: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    import numpy as np
    import pandas as pd
    from six import string_types, integer_types
    import datetime
    import decimal
    import sys

    # If it's one of our types, we use our own conversion; this can move to full schema (see DataAssetIdentifier below)
    # once nesting goes all the way down
    if isinstance(data, (ExpectationConfiguration, ExpectationSuite, ExpectationValidationResult,
                         ExpectationSuiteValidationResult, DataAssetIdentifier)):
        return

    try:
        if not isinstance(data, list) and np.isnan(data):
            # np.isnan is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, (string_types, integer_types, float, bool)):
        # No problem to encode json
        return

    elif isinstance(data, dict):
        for key in data:
            str(key)  # key must be cast-able to string
            ensure_json_serializable(data[key])

        return

    elif isinstance(data, (list, tuple, set)):
        for val in data:
            ensure_json_serializable(val)
        return

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        dummy = [ensure_json_serializable(x) for x in data.tolist()]
        return

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        dummy = [{index_name: ensure_json_serializable(idx), value_name: ensure_json_serializable(val)}
                 for idx, val in data.iteritems()]
        return
    elif isinstance(data, pd.DataFrame):
        return ensure_json_serializable(data.to_dict(orient='records'))

    elif isinstance(data, decimal.Decimal):
        return

    else:
        raise InvalidExpectationConfigurationError('%s is of type %s which cannot be serialized to json' % (
            str(data), type(data).__name__))


class DataContextKey(ABC):
    """DataContextKey objects are used to uniquely identify resources used by the DataContext.

    A DataContextKey is designed to support clear naming with multiple representations including a hashable
    version making it suitable for use as the key in a dictionary.
    """
    @abstractmethod
    def to_tuple(self):
        pass

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(*tuple_)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.to_tuple() == other.to_tuple()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.to_tuple())


class DataAssetIdentifier(DataContextKey):

    prioritized_valid_delimiter_list = ["/", "."]

    def __init__(self, datasource, generator, generator_asset, delimiter='/'):
        self._datasource = datasource
        self._generator = generator
        self._generator_asset = generator_asset
        if delimiter not in self.prioritized_valid_delimiter_list:
            raise InvalidDataContextKeyError("'%s' is not a valid delimiter for DataAssetIdentifier; valid delimiters"
                                             "are %s" % (delimiter, str(self.prioritized_valid_delimiter_list)))
        self._delimiter = delimiter

    @property
    def datasource(self):
        return self._datasource

    @property
    def generator(self):
        return self._generator

    @property
    def generator_asset(self):
        return self._generator_asset

    def to_tuple(self):
        return self.datasource, self.generator, self.generator_asset

    #####
    # DataAssetIdentifier is treated specially to make it very string friendly.
    # Use DataAssetIdentifierSchema to load from a string
    #####

    def to_path(self):
        return "/".join((
            self.datasource.replace("/", "__"),
            self.generator.replace("/", "__"),
            self.generator_asset.replace("/", "__")
        ))

    def __str__(self):
        return self._delimiter.join((self.datasource, self.generator, self.generator_asset))

    def __repr__(self):
        return self._delimiter.join((self.datasource, self.generator, self.generator_asset))


class DataAssetIdentifierSchema(Schema):
    datasource = fields.Str(required=True)
    generator = fields.Str(required=True)
    generator_asset = fields.Str(required=True)

    def parse_from_string(self, data):
        if isinstance(data, string_types):
            for delimiter in DataAssetIdentifier.prioritized_valid_delimiter_list:
                if data.count(delimiter) == 2:
                    data_split = data.split(delimiter)
                    return {
                        "datasource": data_split[0],
                        "generator": data_split[1],
                        "generator_asset": data_split[2]
                    }
            raise ValidationError("No valid delimiter found to parse DataAssetIdentifier: tried %s"
                                  % str(DataAssetIdentifier.prioritized_valid_delimiter_list))
        return data

    # noinspection PyUnusedLocal
    @pre_load(pass_many=False)
    def load_handle_string(self, data, **kwargs):
        return self.parse_from_string(data)

    # noinspection PyUnusedLocal
    @post_load(pass_many=False)
    def make_data_asset_identifier(self, data, **kwargs):
        return DataAssetIdentifier(**data)

    # noinspection PyUnusedLocal
    @pre_dump
    def dump_handle_string(self, data, **kwargs):
        return self.parse_from_string(data)


class ExpectationKwargs(dict):
    ignored_keys = ['result_format', 'include_config', 'catch_exceptions']

    """ExpectationKwargs store information necessary to evaluate an expectation."""
    def __init__(self, *args, **kwargs):
        include_config = kwargs.pop("include_config", None)
        if include_config is not None and not isinstance(include_config, bool):
            raise InvalidExpectationKwargsError("include_config must be a boolean value")

        result_format = kwargs.get("result_format", None)
        if result_format is None:
            pass
        elif result_format in RESULT_FORMATS:
            pass
        elif isinstance(result_format, dict) and result_format.get('result_format', None) in RESULT_FORMATS:
            pass
        else:
            raise InvalidExpectationKwargsError("result format must be one of the valid formats: %s"
                                                % str(RESULT_FORMATS))

        catch_exceptions = kwargs.pop("catch_exceptions", None)
        if catch_exceptions is not None and not isinstance(catch_exceptions, bool):
            raise InvalidExpectationKwargsError("catch_exceptions must be a boolean value")

        super(ExpectationKwargs, self).__init__(*args, **kwargs)
        ensure_json_serializable(self)

    def isEquivalentTo(self, other):
        try:
            return len(self) == len(other) and all([
                self[k] == other[k] for k in self.keys() if k not in self.ignored_keys
            ])
        except KeyError:
            return False

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = convert_to_json_serializable(self)
        return myself


class ExpectationConfiguration(DictDot):
    """ExpectationConfiguration defines the parameters and name of a specific expectation."""

    def __init__(self, expectation_type, kwargs, meta=None, success_on_last_run=None):
        if not isinstance(expectation_type, str):
            raise InvalidExpectationConfigurationError("expectation_type must be a string")
        self._expectation_type = expectation_type
        if not isinstance(kwargs, dict):
            raise InvalidExpectationConfigurationError("expectation configuration kwargs must be an "
                                                       "ExpectationKwargs object.")
        self._kwargs = ExpectationKwargs(kwargs)
        if meta is None:
            meta = {}
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta
        self.success_on_last_run = success_on_last_run

    @property
    def expectation_type(self):
        return self._expectation_type

    @property
    def kwargs(self):
        return self._kwargs

    def isEquivalentTo(self, other):
        """ExpectationConfiguration equivalence does not include meta, and relies on *equivalence* of kwargs."""
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other = expectationConfigurationSchema.load(other).data
                except ValidationError:
                    logger.debug("Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                                 "dict other could not be instantiated as an ExpectationConfiguration")
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented
        return all((
            self.expectation_type == other.expectation_type,
            self.kwargs.isEquivalentTo(other.kwargs)
        ))

    def __eq__(self, other):
        """ExpectationConfiguration equality does include meta, but ignores instance identity."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all((
            self.expectation_type == other.expectation_type,
            self.kwargs == other.kwargs,
            self.meta == other.meta
        ))

    def __ne__(self, other):
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = expectationConfigurationSchema.dump(self).data
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself['kwargs'] = convert_to_json_serializable(myself['kwargs'])
        return myself


class ExpectationConfigurationSchema(Schema):
    expectation_type = fields.Str(
        required=True,
        error_messages={"required": "expectation_type missing in expectation configuration"}
    )
    kwargs = fields.Dict()
    meta = fields.Dict()

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_configuration(self, data, **kwargs):
        return ExpectationConfiguration(**data)


# TODO: re-enable once we can allow arbitrary keys but still add this sort of validation
# class MetaDictSchema(Schema):
#     """The MetaDict """
#
#     # noinspection PyUnusedLocal
#     @validates_schema
#     def validate_json_serializable(self, data, **kwargs):
#         import json
#         try:
#             json.dumps(data)
#         except (TypeError, OverflowError):
#             raise ValidationError("meta information must be json serializable.")


class ExpectationSuite(object):
    def __init__(
            self,
            data_asset_name, expectation_suite_name,
            expectations=None, evaluation_parameters=None, data_asset_type=None, meta=None):
        self.data_asset_name = data_asset_name
        self.expectation_suite_name = expectation_suite_name
        if expectations is None:
            expectations = []
        self.expectations = [ExpectationConfiguration(**expectation) if isinstance(expectation, dict) else
                             expectation for expectation in expectations]
        if evaluation_parameters is None:
            evaluation_parameters = {}
        self.evaluation_parameters = evaluation_parameters
        self.data_asset_type = data_asset_type
        if meta is None:
            meta = {"great_expectations.__version__": ge_version}
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta

    def isEquivalentTo(self, other):
        """
        ExpectationSuite equivalence relies only on expectations and evaluation parameters. It does not include:
        - data_asset_name
        - expectation_suite_name
        - meta
        - data_asset_type
        """
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other = expectationSuiteSchema.load(other).data
                except ValidationError:
                    logger.debug("Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                                 "dict other could not be instantiated as an ExpectationConfiguration")
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented
        return all(
            [mine.isEquivalentTo(theirs) for (mine, theirs) in zip(self.expectations, other.expectations)]
        )

    def __eq__(self, other):
        """ExpectationSuite equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all((
            self.data_asset_name == other.data_asset_name,
            self.expectation_suite_name == other.expectation_suite_name,
            self.expectations == other.expectations,
            self.evaluation_parameters == other.evaluation_parameters,
            self.data_asset_type == other.data_asset_type,
            self.meta == other.meta,

        ))

    def __ne__(self, other):
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = expectationSuiteSchema.dump(self).data
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself['expectations'] = convert_to_json_serializable(myself['expectations'])
        try:
            myself['evaluation_parameters'] = convert_to_json_serializable(myself['evaluation_parameters'])
        except KeyError:
            pass  # Allow evaluation parameters to be missing if empty
        myself['meta'] = convert_to_json_serializable(myself['meta'])
        return myself


class NamespaceAwareExpectationSuite(ExpectationSuite):
    def __init__(
            self,
            data_asset_name, expectation_suite_name,
            expectations=None, evaluation_parameters=None, data_asset_type=None, meta=None):
        if not isinstance(data_asset_name, DataAssetIdentifier):
            raise ValueError("NamespaceAwareExpectationSuite requires a DataAssetIdentifier as its data_asset_name.")
        super(NamespaceAwareExpectationSuite, self).__init__(
            data_asset_name, expectation_suite_name, expectations, evaluation_parameters, data_asset_type, meta
        )

    def isEquivalentTo(self, other):
        """
        ExpectationSuite equivalence relies only on expectations and evaluation parameters. It does not include:
        - data_asset_name
        - expectation_suite_name
        - meta
        - data_asset_type
        """
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other = namespaceAwareExpectationSuiteSchema.load(other).data
                except ValidationError:
                    logger.debug("Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                                 "dict other could not be instantiated as an ExpectationConfiguration")
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented
        return all(
            [mine.isEquivalentTo(theirs) for (mine, theirs) in zip(self.expectations, other.expectations)]
        )

    def to_json_dict(self):
        myself = namespaceAwareExpectationSuiteSchema.dump(self).data
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself['expectations'] = convert_to_json_serializable(myself['expectations'])
        try:
            myself['evaluation_parameters'] = convert_to_json_serializable(myself['evaluation_parameters'])
        except KeyError:
            pass  # Allow evaluation parameters to be missing if empty
        myself['meta'] = convert_to_json_serializable(myself['meta'])
        return myself


class ExpectationSuiteSchema(Schema):
    data_asset_name = fields.Str()
    expectation_suite_name = fields.Str()
    expectations = fields.List(fields.Nested(ExpectationConfigurationSchema))
    evaluation_parameters = fields.Dict(allow_none=True)
    data_asset_type = fields.Str(allow_none=True)
    meta = fields.Dict()

    # NOTE: 20191107 - JPC - we may want to remove clean_empty and update tests to require the other fields;
    # doing so could also allow us not to have to make a copy of data in the pre_dump method.
    def clean_empty(self, data):
        if not hasattr(data, 'evaluation_parameters'):
            pass
        elif len(data.evaluation_parameters) == 0:
            del data.evaluation_parameters
        if not hasattr(data, 'meta'):
            pass
        elif len(data.meta) == 0:
            del data.meta
        return data

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        data.meta = convert_to_json_serializable(data.meta)
        data = self.clean_empty(data)
        if isinstance(data.data_asset_name, DataAssetIdentifier):
            logger.warning("Dumping a NamespaceAwareExpectationSuite using ExpectationSuiteSchema. Consider using "
                           "NamespaceAwareExpectationSuiteSchema instead.")
            data.data_asset_name = DataAssetIdentifier.to_path(data.data_asset_name)
        return data

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite(self, data, **kwargs):
        return ExpectationSuite(**data)


class NamespaceAwareExpectationSuiteSchema(ExpectationSuiteSchema):
    data_asset_name = fields.Nested(DataAssetIdentifierSchema)

    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = self.clean_empty(deepcopy(data))
        # if isinstance(data.data_asset_name, DataAssetIdentifier):
        #     logger.warning("Dumping a NamespaceAwareExpectationSuite using ExpectationSuiteSchema. Consider using "
        #                    "NamespaceAwareExpectationSuiteSchema instead.")
        #     data.data_asset_name = DataAssetIdentifier.to_path(data.data_asset_name)
        return data

    @post_load
    def make_expectation_suite(self, data, **kwargs):
        return NamespaceAwareExpectationSuite(**data)


class ExpectationValidationResult(object):
    def __init__(self, success=None, expectation_config=None, result=None, meta=None, exception_info=None):
        self.success = success
        self.expectation_config = expectation_config
        # TODO: re-add
        # assert_json_serializable(result, "result")
        if result is None:
            result = {}
        self.result = result
        if meta is None:
            meta = {}
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta
        self.exception_info = exception_info

    def __eq__(self, other):
        """ExpectationValidationResult equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        try:
            return all((
                self.success == other.success,
                self.expectation_config == other.expectation_config,
                # Result is a dictionary allowed to have nested dictionaries that are still of complex types (e.g.
                # numpy) consequently, series' comparison can persist. Wrapping in all() ensures comparision is
                # handled appropriately.
                (self.result is None and other.result is None) or (all(self.result) == all(other.result)),
                self.meta == other.meta,
                self.exception_info == other.exception_info
            ))
        except (ValueError, TypeError):
            # if invalid comparisons are attempted, the objects are not equal.
            return False

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = expectationValidationResultSchema.dump(self).data
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        if 'result' in myself:
            myself['result'] = convert_to_json_serializable(myself['result'])
        if 'meta' in myself:
            myself['meta'] = convert_to_json_serializable(myself['meta'])
        if 'exception_info' in myself:
            myself['exception_info'] = convert_to_json_serializable(myself['exception_info'])
        return myself


class ExpectationValidationResultSchema(Schema):
    success = fields.Bool()
    expectation_config = fields.Nested(ExpectationConfigurationSchema)
    result = fields.Dict()
    meta = fields.Dict()
    exception_info = fields.Dict()

    # noinspection PyUnusedLocal
    @pre_dump
    def convert_result_to_serializable(self, data, **kwargs):
        data = deepcopy(data)
        data.result = convert_to_json_serializable(data.result)
        return data

    # # noinspection PyUnusedLocal
    # @pre_dump
    # def clean_empty(self, data, **kwargs):
    #     # if not hasattr(data, 'meta'):
    #     #     pass
    #     # elif len(data.meta) == 0:
    #     #     del data.meta
    #     # return data
    #     pass

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_validation_result(self, data, **kwargs):
        return ExpectationValidationResult(**data)


class ExpectationSuiteValidationResult(DictDot):
    def __init__(self, success=None, results=None, evaluation_parameters=None, statistics=None, meta=None):
        self.success = success
        if results is None:
            results = []
        self.results = results
        if evaluation_parameters is None:
            evaluation_parameters = {}
        self.evaluation_parameters = evaluation_parameters
        if statistics is None:
            statistics = {}
        self.statistics = statistics
        if meta is None:
            meta = {}
        ensure_json_serializable(meta)  # We require meta information to be serializable.
        self.meta = meta

    def __eq__(self, other):
        """ExpectationSuiteValidationResult equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all((
            self.success == other.success,
            self.results == other.results,
            self.evaluation_parameters == other.evaluation_parameters,
            self.statistics == other.statistics,
            self.meta == other.meta
        ))

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = deepcopy(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself['evaluation_parameters'] = convert_to_json_serializable(myself['evaluation_parameters'])
        myself['statistics'] = convert_to_json_serializable(myself['statistics'])
        myself['meta'] = convert_to_json_serializable(myself['meta'])
        myself = expectationSuiteValidationResultSchema.dump(myself).data
        return myself


class ExpectationSuiteValidationResultSchema(Schema):
    success = fields.Bool()
    results = fields.List(fields.Nested(ExpectationValidationResultSchema))
    evaluation_parameters = fields.Dict()
    statistics = fields.Dict()
    meta = fields.Dict(allow_none=True)

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        data.meta = convert_to_json_serializable(data.meta)
        return data

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite_validation_result(self, data, **kwargs):
        return ExpectationSuiteValidationResult(**data)


dataAssetIdentifierSchema = DataAssetIdentifierSchema(strict=True)
expectationConfigurationSchema = ExpectationConfigurationSchema(strict=True)
expectationSuiteSchema = ExpectationSuiteSchema(strict=True)
namespaceAwareExpectationSuiteSchema = NamespaceAwareExpectationSuiteSchema(strict=True)
expectationValidationResultSchema = ExpectationValidationResultSchema(strict=True)
expectationSuiteValidationResultSchema = ExpectationSuiteValidationResultSchema(strict=True)
