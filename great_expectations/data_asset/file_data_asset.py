from __future__ import division
import re
import inspect
import hashlib
import os
import json
import jsonschema
from functools import wraps
import numpy as np
from six import PY3
from itertools import compress
from .base import DataAsset
from .util import parse_result_format



class MetaFileDataAsset(DataAsset):
    """MetaFileDataset is a thin layer above FileDataset.
    This two-layer inheritance is required to make @classmethod decorators work.
    Practically speaking, that means that MetaFileDataset implements \
    expectation decorators, like `file_lines_map_expectation` \
    and FileDataset implements the expectation methods themselves.
    """

    def __init__(self, *args, **kwargs):
        super(MetaFileDataAsset, self).__init__(*args, **kwargs)

    @classmethod
    def file_lines_map_expectation(cls, func):
        """Constructs an expectation using file lines map semantics.
        The file_lines_map_expectations decorator handles boilerplate issues
        surrounding the common pattern of evaluating truthiness of some
        condition on an line by line basis in a file.

        Args:
            func (function): \
                The function implementing an expectation that will be applied
                line by line across a file. The function should take a file
                and return information about how many lines met expectations.

        Notes:
            Users can specify skip value k that will cause the expectation
            function to disregard the first k lines of the file

        See also:
            :func:`expect_file_line_regex_match_count_to_be_between
            <great_expectations.data_asset.base.DataAsset.expect_file_line_regex_match_count_to_be_between>` \
            for an example of a file_lines_map_expectation
        """
        if PY3:
            argspec = inspect.getfullargspec(func)[0][1:]
        else:
            argspec = inspect.getargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, mostly=None, skip=None, result_format=None, *args, **kwargs):
            try:
                f = open(self.path, "r")
            except:
                raise

            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)
            lines = f.readlines() #Read in file lines

            #Skip k initial lines designated by the user
            if skip is not None and skip <= len(lines):
                try:
                    assert float(skip).is_integer()
                    assert float(skip) >= 0
                except:
                    raise ValueError("skip must be a positive integer")

                for i in range(1, skip+1):
                    lines.pop(0)

            if lines:
                null_lines = re.compile("\s+") #Ignore lines with just white space
                boolean_mapped_null_lines = np.array(
                    [bool(null_lines.match(line)) for line in lines])
                element_count = int(len(lines))
                if element_count > sum(boolean_mapped_null_lines):
                    nonnull_lines = list(compress(lines, np.invert(boolean_mapped_null_lines)))
                    nonnull_count = int((boolean_mapped_null_lines == False).sum())
                    boolean_mapped_success_lines = np.array(
                        func(self, lines=nonnull_lines, *args, **kwargs))
                    success_count = np.count_nonzero(boolean_mapped_success_lines)
                    unexpected_list = list(compress(nonnull_lines, \
                        np.invert(boolean_mapped_success_lines)))
                    nonnull_lines_index = range(0, len(nonnull_lines)+1)
                    unexpected_index_list = list(compress(nonnull_lines_index,
                                                          np.invert(boolean_mapped_success_lines)))
                    success, percent_success = self._calc_map_expectation_success(
                        success_count, nonnull_count, mostly)
                    return_obj = self._format_map_output(
                        result_format, success,
                        element_count, nonnull_count,
                        unexpected_list, unexpected_index_list
                    )
                else:
                    return_obj = self._format_map_output(
                        result_format=result_format, success=None,
                        element_count=element_count, nonnull_count=0,
                        unexpected_list=[], unexpected_index_list=[]
                    )
            else:
                return_obj = self._format_map_output(result_format=result_format,
                                                     success=None,
                                                     element_count=0,
                                                     nonnull_count=0,
                                                     unexpected_list=[],
                                                     unexpected_index_list=[])
            f.close()
            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper

class FileDataAsset(MetaFileDataAsset):
    """
    FileDataset instantiates the great_expectations Expectations API as a
    subclass of a python file object. For the full API reference, please see
    :func:`DataAsset <great_expectations.data_asset.base.DataAsset>`
    """


    def __init__(self, file_path, *args, **kwargs):
        super(FileDataAsset, self).__init__(*args, **kwargs)
        self.path = file_path


    @MetaFileDataAsset.file_lines_map_expectation
    def expect_file_line_regex_match_count_to_be_between(self,
                                                         regex, lines=None,
                                                         skip=None,
                                                         expected_min_count=0,
                                                         expected_max_count=None,
                                                         mostly=None,
                                                         result_format=None,
                                                         include_config=False,
                                                         catch_exceptions=None,
                                                         meta=None):
        """
        Expect the number of times a regular expression appears on each line of
        a file to be between a maximum and minimum value.

        Args: regex:
            A string that can be compiled as valid regular expression

        Keyword Args:
            lines: \
                An empty variable that recieves the file lines from the
                file_lines_map_expectation method. It doesn't matter what the
                user gives for this value as it will be replaced with the lines
                of the file by file_lines_map_expecation. It is recommended the
                user ignore this argument and leave it at its default.

            skip (nonnegative integer): \
                Integer specifying the first lines in the file the method should
                skip before assessing expectations

            expected_min_count (None or nonnegative integer): \
                Specifies the minimum number of times regex is expected to appear
                on each line of the file

            expected_max_count (None or nonnegative integer): \
               Specifies the maximum number of times regex is expected to appear
               on each line of the file

            mostly (None or number between 0 and 1): \

                Specifies an acceptable error for expectations. If the percentage
                of unexpected lines is less than mostly, the method still returns
                true even if all lines don't match the expectation criteria.

            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`,
                or `SUMMARY`. For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the
                result object. For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the
                result object. For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be
                included in the output without modification. For more detail,
                see :ref:`meta`.

        Returns:

            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to
            :ref:`result_format <result_format>` and :ref:`include_config`,
            :ref:`catch_exceptions`, and :ref:`meta`.

        """
        try:
            comp_regex = re.compile(regex)
        except:
            raise ValueError("Must enter valid regular expression for regex")

        if expected_min_count != None:
            try:
                assert float(expected_min_count).is_integer()
                assert float(expected_min_count) >= 0
            except:
                raise ValueError("expected_min_count must be a non-negative \
                                 integer or None")

        if expected_max_count != None:
            try:
                assert float(expected_max_count).is_integer()
                assert float(expected_max_count) >= 0
            except:
                raise ValueError("expected_max_count must be a non-negative \
                                 integer or None")

        if expected_max_count != None and expected_min_count != None:
            try:
                assert expected_max_count >= expected_min_count
            except:
                raise ValueError("expected_max_count must be greater than or \
                                 equal to expected_min_count")

        if expected_max_count != None and expected_min_count != None:
            truth_list = [True if(len(comp_regex.findall(line)) >= expected_min_count and \
                                len(comp_regex.findall(line)) <= expected_max_count) else False \
                                for line in lines]

        elif expected_max_count != None:
            truth_list = [True if(len(comp_regex.findall(line)) <= expected_max_count) else False \
                                for line in lines]

        elif expected_min_count != None:
              truth_list = [True if(len(comp_regex.findall(line)) >= expected_min_count) else False \
                                for line in lines]
        else:
            truth_list = [True for line in lines]

        return truth_list
    
    @MetaFileDataAsset.file_lines_map_expectation
    def expect_file_line_regex_match_count_to_equal(self, regex, lines=None,
                                                    expected_count=0, skip=None,
                                                    mostly=None, result_format=None,
                                                    include_config=False,
                                                    catch_exceptions=None, meta=None):

        """
        Expect the number of times a regular expression appears on each line of a file to equal some
        expected count

        Args: regex:
            A string that can be compiled as valid regular expression

        Keyword Args:
            lines: \
                An empty variable that recieves the file lines from the
                file_lines_map_expectation method. It doesn't matter what the
                user gives for this value as it will be replaced with the lines
                of the file by file_lines_map_expecation. It is recommended the
                user ignore this argument and leave it at its default.

            skip (nonnegative integer): \
                Integer specifying the first lines in the file the method should
                skip before assessing expectations

            expected_count (None or nonnegative integer): \
                Specifies the number of times regex is expected to appear on each
                line of the file


            mostly (None or number between 0 and 1): \

                Specifies an acceptable error for expectations. If the percentage
                of unexpected lines is less than mostly, the method still returns
                true even if all lines don't match the expectation criteria.

            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`,
                or `SUMMARY`. For more detail, see :ref:`result_format
                <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the result
                object. For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the
                result object. For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be
                included in the output without modification. For more detail,
                see :ref:`meta`.

        Returns:

            A JSON-serializable expectation result object.

            Exact fields vary depending on the values passed to :ref:`result_format
            <result_format>` and :ref:`include_config`, :ref:`catch_exceptions`,
            and :ref:`meta`.

        """
        try:
            comp_regex = re.compile(regex)
        except:
            raise ValueError("Must enter valid regular expression for regex")

        try:
            assert float(expected_count).is_integer()
            assert float(expected_count) >= 0

        except:
            raise ValueError("expected_count must be a non-negative integer")

        return [True if(len(comp_regex.findall(line)) == expected_count) else False \
                                for line in lines]

    @DataAsset.expectation(["value"])
    def expect_file_hash_to_equal(self, value, hash_alg='md5', result_format=None,
                                  include_config=False, catch_exceptions=None,
                                  meta=None):

        """
        Expect computed file hash to equal some given value.

        Args:
            value: A string to compare with the computed hash value

        Keyword Args:
            hash_alg (string):  Indicates the hash algorithm to use

            result_format (str or None): \
                Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`,
                or `SUMMARY`. For more detail, see :ref:`result_format <result_format>`.
            include_config (boolean): \
                If True, then include the expectation config as part of the
                result object. For more detail, see :ref:`include_config`.
            catch_exceptions (boolean or None): \
                If True, then catch exceptions and include them as part of the result object. \
                For more detail, see :ref:`catch_exceptions`.
            meta (dict or None): \
                A JSON-serializable dictionary (nesting allowed) that will be
                included in the output without modification. For more detail,
                see :ref:`meta`.

        Returns:
            A JSON-serializable expectation result object.

        Exact fields vary depending on the values passed to :ref:`result_format
        <result_format>` and :ref:`include_config`, :ref:`catch_exceptions`,
        and :ref:`meta`.
        """
        success = False
        try:
            hash = hashlib.new(hash_alg)

        # Limit file reads to 64 KB chunks at a time
            BLOCKSIZE = 65536
            try:
                with open(self.path, 'rb') as file:
                    file_buffer = file.read(BLOCKSIZE)
                    while file_buffer:
                        hash.update(file_buffer)
                        file_buffer = file.read(BLOCKSIZE)
                    success = hash.hexdigest() == value
            except IOError:
                raise
        except ValueError:
            raise
        return {"success":success}

