
import hashlib
import inspect
import json
import os
import re
from functools import wraps
from itertools import compress
import jsonschema
import numpy as np
from great_expectations.data_asset.data_asset import DataAsset
from great_expectations.data_asset.util import parse_result_format

class MetaFileDataAsset(DataAsset):
    'MetaFileDataset is a thin layer above FileDataset.\n    This two-layer inheritance is required to make @classmethod decorators work.\n    Practically speaking, that means that MetaFileDataset implements     expectation decorators, like `file_lines_map_expectation`     and FileDataset implements the expectation methods themselves.\n    '

    def __init__(self, *args, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(*args, **kwargs)

    @classmethod
    def file_lines_map_expectation(cls, func):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Constructs an expectation using file lines map semantics.\n        The file_lines_map_expectations decorator handles boilerplate issues\n        surrounding the common pattern of evaluating truthiness of some\n        condition on an line by line basis in a file.\n\n        Args:\n            func (function):                 The function implementing an expectation that will be applied\n                line by line across a file. The function should take a file\n                and return information about how many lines met expectations.\n\n        Notes:\n            Users can specify skip value k that will cause the expectation\n            function to disregard the first k lines of the file.\n\n            file_lines_map_expectation will add a kwarg _lines to the called function with the nonnull lines             to process.\n\n            null_lines_regex defines a regex used to skip lines, but can be overridden\n\n        See also:\n            :func:`expect_file_line_regex_match_count_to_be_between\n            <great_expectations.data_asset.base.DataAsset.expect_file_line_regex_match_count_to_be_between>`             for an example of a file_lines_map_expectation\n        '
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, skip=None, mostly=None, null_lines_regex='^\\s*$', result_format=None, *args, **kwargs):
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            try:
                f = open(self._path)
            except OSError:
                raise
            if (result_format is None):
                result_format = self.default_expectation_args['result_format']
            result_format = parse_result_format(result_format)
            lines = f.readlines()
            if ((skip is not None) and (skip <= len(lines))):
                try:
                    assert float(skip).is_integer()
                    assert (float(skip) >= 0)
                except (AssertionError, ValueError):
                    raise ValueError('skip must be a positive integer')
                for i in range(1, (skip + 1)):
                    lines.pop(0)
            if lines:
                if (null_lines_regex is not None):
                    null_lines = re.compile(null_lines_regex)
                    boolean_mapped_null_lines = np.array([bool(null_lines.match(line)) for line in lines])
                else:
                    boolean_mapped_null_lines = np.zeros(len(lines), dtype=bool)
                element_count = int(len(lines))
                if (element_count > sum(boolean_mapped_null_lines)):
                    nonnull_lines = list(compress(lines, np.invert(boolean_mapped_null_lines)))
                    nonnull_count = int((boolean_mapped_null_lines == False).sum())
                    boolean_mapped_success_lines = np.array(func(self, *args, _lines=nonnull_lines, **kwargs))
                    success_count = np.count_nonzero(boolean_mapped_success_lines)
                    unexpected_list = list(compress(nonnull_lines, np.invert(boolean_mapped_success_lines)))
                    nonnull_lines_index = range(0, (len(nonnull_lines) + 1))
                    unexpected_index_list = list(compress(nonnull_lines_index, np.invert(boolean_mapped_success_lines)))
                    (success, percent_success) = self._calc_map_expectation_success(success_count, nonnull_count, mostly)
                    return_obj = self._format_map_output(result_format, success, element_count, nonnull_count, len(unexpected_list), unexpected_list, unexpected_index_list)
                else:
                    return_obj = self._format_map_output(result_format=result_format, success=None, element_count=element_count, nonnull_count=0, unexpected_count=0, unexpected_list=[], unexpected_index_list=[])
            else:
                return_obj = self._format_map_output(result_format=result_format, success=None, element_count=0, nonnull_count=0, unexpected_count=0, unexpected_list=[], unexpected_index_list=[])
            f.close()
            return return_obj
        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__
        return inner_wrapper

class FileDataAsset(MetaFileDataAsset):
    '\n    FileDataset instantiates the great_expectations Expectations API as a\n    subclass of a python file object. For the full API reference, please see\n    :func:`DataAsset <great_expectations.data_asset.base.DataAsset>`\n    '
    _data_asset_type = 'FileDataAsset'

    def __init__(self, file_path=None, *args, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(*args, **kwargs)
        self._path = file_path

    @MetaFileDataAsset.file_lines_map_expectation
    def expect_file_line_regex_match_count_to_be_between(self, regex, expected_min_count=0, expected_max_count=None, skip=None, mostly=None, null_lines_regex='^\\s*$', result_format=None, include_config=True, catch_exceptions=None, meta=None, _lines=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "\n        Expect the number of times a regular expression appears on each line of\n        a file to be between a maximum and minimum value.\n\n        Args:\n            regex:                 A string that can be compiled as valid regular expression to match\n\n            expected_min_count (None or nonnegative integer):                 Specifies the minimum number of times regex is expected to appear\n                on each line of the file\n\n            expected_max_count (None or nonnegative integer):                Specifies the maximum number of times regex is expected to appear\n               on each line of the file\n        Keyword Args:\n            skip (None or nonnegative integer):                 Integer specifying the first lines in the file the method should\n                skip before assessing expectations\n\n            mostly (None or number between 0 and 1):                 Specifies an acceptable error for expectations. If the percentage\n                of unexpected lines is less than mostly, the method still returns\n                true even if all lines don't match the expectation criteria.\n\n            null_lines_regex (valid regular expression or None):                 If not none, a regex to skip lines as null. Defaults to empty or whitespace-only lines.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`,\n                or `SUMMARY`. For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the\n                result object. For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the\n                result object. For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be\n                included in the output without modification. For more detail,\n                see :ref:`meta`.\n            _lines (list):                 The lines over which to operate (provided by the file_lines_map_expectation decorator)\n\n        Returns:\n\n            A JSON-serializable expectation result object.\n\n            Exact fields vary depending on the values passed to\n            :ref:`result_format <result_format>` and :ref:`include_config`,\n            :ref:`catch_exceptions`, and :ref:`meta`.\n\n        "
        try:
            comp_regex = re.compile(regex)
        except (ValueError, TypeError):
            raise ValueError('Must enter valid regular expression for regex')
        if (expected_min_count is not None):
            try:
                assert float(expected_min_count).is_integer()
                assert (float(expected_min_count) >= 0)
            except (AssertionError, ValueError):
                raise ValueError('expected_min_count must be a non-negative                                  integer or None')
        if (expected_max_count is not None):
            try:
                assert float(expected_max_count).is_integer()
                assert (float(expected_max_count) >= 0)
            except (AssertionError, ValueError):
                raise ValueError('expected_max_count must be a non-negative                                  integer or None')
        if ((expected_max_count is not None) and (expected_min_count is not None)):
            try:
                assert (expected_max_count >= expected_min_count)
            except (AssertionError, ValueError):
                raise ValueError('expected_max_count must be greater than or                                  equal to expected_min_count')
        if ((expected_max_count is not None) and (expected_min_count is not None)):
            truth_list = [(expected_min_count <= len(comp_regex.findall(line)) <= expected_max_count) for line in _lines]
        elif (expected_max_count is not None):
            truth_list = [(len(comp_regex.findall(line)) <= expected_max_count) for line in _lines]
        elif (expected_min_count is not None):
            truth_list = [(len(comp_regex.findall(line)) >= expected_min_count) for line in _lines]
        else:
            truth_list = [True for _ in _lines]
        return truth_list

    @MetaFileDataAsset.file_lines_map_expectation
    def expect_file_line_regex_match_count_to_equal(self, regex, expected_count=0, skip=None, mostly=None, nonnull_lines_regex='^\\s*$', result_format=None, include_config=True, catch_exceptions=None, meta=None, _lines=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "\n        Expect the number of times a regular expression appears on each line of\n        a file to be between a maximum and minimum value.\n\n        Args:\n            regex:                 A string that can be compiled as valid regular expression to match\n\n            expected_count (None or nonnegative integer):                 Specifies the number of times regex is expected to appear on each\n                line of the file\n        Keyword Args:\n            skip (None or nonnegative integer):                 Integer specifying the first lines in the file the method should\n                skip before assessing expectations\n\n            mostly (None or number between 0 and 1):                 Specifies an acceptable error for expectations. If the percentage\n                of unexpected lines is less than mostly, the method still returns\n                true even if all lines don't match the expectation criteria.\n\n            nonnull_lines_regex (valid regular expression or None):                 If not none, a regex to skip lines as null. Defaults to empty or whitespace-only lines.\n\n        Other Parameters:\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`,\n                or `SUMMARY`. For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the\n                result object. For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the\n                result object. For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be\n                included in the output without modification. For more detail,\n                see :ref:`meta`.\n            _lines (list):                 The lines over which to operate (provided by the file_lines_map_expectation decorator)\n\n        Returns:\n\n            A JSON-serializable expectation result object.\n\n            Exact fields vary depending on the values passed to\n            :ref:`result_format <result_format>` and :ref:`include_config`,\n            :ref:`catch_exceptions`, and :ref:`meta`.\n\n        "
        try:
            comp_regex = re.compile(regex)
        except (ValueError, TypeError):
            raise ValueError('Must enter valid regular expression for regex')
        try:
            assert float(expected_count).is_integer()
            assert (float(expected_count) >= 0)
        except (AssertionError, ValueError):
            raise ValueError('expected_count must be a non-negative integer')
        return [(len(comp_regex.findall(line)) == expected_count) for line in _lines]

    @DataAsset.expectation(['value'])
    def expect_file_hash_to_equal(self, value, hash_alg='md5', result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect computed file hash to equal some given value.\n\n        Args:\n            value: A string to compare with the computed hash value\n\n        Keyword Args:\n            hash_alg (string):  Indicates the hash algorithm to use\n\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`,\n                or `SUMMARY`. For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the\n                result object. For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be\n                included in the output without modification. For more detail,\n                see :ref:`meta`.\n\n        Returns:\n            A JSON-serializable expectation result object.\n\n        Exact fields vary depending on the values passed to :ref:`result_format\n        <result_format>` and :ref:`include_config`, :ref:`catch_exceptions`,\n        and :ref:`meta`.\n        '
        success = False
        try:
            hash = hashlib.new(hash_alg)
            BLOCK_SIZE = 65536
            try:
                with open(self._path, 'rb') as file:
                    file_buffer = file.read(BLOCK_SIZE)
                    while file_buffer:
                        hash.update(file_buffer)
                        file_buffer = file.read(BLOCK_SIZE)
                    success = (hash.hexdigest() == value)
            except OSError:
                raise
        except ValueError:
            raise
        return {'success': success}

    @DataAsset.expectation(['minsize', 'maxsize'])
    def expect_file_size_to_be_between(self, minsize=0, maxsize=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Expect file size to be between a user specified maxsize and minsize.\n\n        Args:\n            minsize(integer): minimum expected file size\n            maxsize(integer): maximum expected file size\n\n        Keyword Args:\n\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be\n                included in the output without modification. For more detail,\n                see :ref:`meta`.\n\n        Returns:\n            A JSON-serializable expectation result object.\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n        '
        try:
            size = os.path.getsize(self._path)
        except OSError:
            raise
        try:
            if (not float(minsize).is_integer()):
                raise ValueError('minsize must be an integer')
            minsize = int(float(minsize))
            if ((maxsize is not None) and (not float(maxsize).is_integer())):
                raise ValueError('maxsize must be an integer')
            elif (maxsize is not None):
                maxsize = int(float(maxsize))
        except TypeError:
            raise
        if (minsize < 0):
            raise ValueError('minsize must be greater than or equal to 0')
        if ((maxsize is not None) and (maxsize < 0)):
            raise ValueError('maxsize must be greater than or equal to 0')
        if ((maxsize is not None) and (minsize > maxsize)):
            raise ValueError('maxsize must be greater than or equal to minsize')
        if ((maxsize is None) and (size >= minsize)):
            success = True
        elif ((size >= minsize) and (size <= maxsize)):
            success = True
        else:
            success = False
        return {'success': success, 'result': {'observed_value': size}}

    @DataAsset.expectation(['filepath'])
    def expect_file_to_exist(self, filepath=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Checks to see if a file specified by the user actually exists\n\n        Args:\n            filepath (str or None):                 The filepath to evaluate. If none, will check the currently-configured path object\n                of this FileDataAsset.\n\n        Keyword Args:\n\n            result_format (str or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be\n                included in the output without modification. For more detail,\n                see :ref:`meta`.\n\n        Returns:\n            A JSON-serializable expectation result object.\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n        '
        if ((filepath is not None) and os.path.isfile(filepath)):
            success = True
        elif ((self._path is not None) and os.path.isfile(self._path)):
            success = True
        else:
            success = False
        return {'success': success}

    @DataAsset.expectation([])
    def expect_file_to_have_valid_table_header(self, regex, skip=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Checks to see if a file has a line with unique delimited values,\n        such a line may be used as a table header.\n\n        Keyword Args:\n            skip (nonnegative integer):                 Integer specifying the first lines in the file the method\n                should skip before assessing expectations\n\n            regex (string):\n                A string that can be compiled as valid regular expression.\n                Used to specify the elements of the table header (the column headers)\n\n            result_format (str or None):\n                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be\n                included in the output without modification. For more detail,\n                see :ref:`meta`.\n\n        Returns:\n            A JSON-serializable expectation result object.\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n        '
        try:
            comp_regex = re.compile(regex)
        except re.error:
            raise ValueError('Must enter valid regular expression for regex')
        success = False
        try:
            with open(self._path) as f:
                lines = f.readlines()
        except OSError:
            raise
        if ((skip is not None) and (skip <= len(lines))):
            try:
                assert float(skip).is_integer()
                assert (float(skip) >= 0)
            except (AssertionError, ValueError):
                raise ValueError('skip must be a positive integer')
            lines = lines[skip:]
        header_line = lines[0].strip()
        header_names = comp_regex.split(header_line)
        if (len(set(header_names)) == len(header_names)):
            success = True
        return {'success': success}

    @DataAsset.expectation([])
    def expect_file_to_be_valid_json(self, schema=None, result_format=None, include_config=True, catch_exceptions=None, meta=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Args:\n            schema : string\n                optional JSON schema file on which JSON data file is validated against\n\n            result_format (str or None):\n                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.                 For more detail, see :ref:`result_format <result_format>`.\n\n            include_config (boolean):\n                If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n\n            catch_exceptions (boolean or None):\n                If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n\n            meta (dict or None):\n                A JSON-serializable dictionary (nesting allowed) that will                 be included in the output without modification.\n\n        For more detail, see :ref:`meta`.\n\n        Returns:\n            A JSON-serializable expectation result object.\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and         :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        '
        if (schema is None):
            try:
                with open(self._path) as f:
                    json.load(f)
                success = True
            except ValueError:
                success = False
        else:
            try:
                with open(schema) as s:
                    schema_data = s.read()
                sdata = json.loads(schema_data)
                with open(self._path) as f:
                    json_data = f.read()
                jdata = json.loads(json_data)
                jsonschema.validate(jdata, sdata)
                success = True
            except jsonschema.ValidationError:
                success = False
            except jsonschema.SchemaError:
                raise
            except:
                raise
        return {'success': success}
