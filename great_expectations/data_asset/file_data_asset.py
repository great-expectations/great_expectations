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
from .util import DocInherit, parse_result_format



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

