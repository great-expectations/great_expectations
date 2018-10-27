# -*- coding: utf-8 -*-
"""
Created on Sat Oct 20 13:49:29 2018

@author: anhol
"""

from __future__ import division

from .base import Dataset
import re
import numpy as np
from itertools import compress
from six import PY3
from .util import DocInherit, parse_result_format
import inspect
from functools import wraps


class MetaFileDataset(Dataset):
    """MetaFileDataset is a thin layer between Dataset and FileDataset.
    This two-layer inheritance is required to make @classmethod decorators work.
    Practically speaking, that means that MetaFileDataset implements \
    expectation decorators, like `column_map_expectation` and `column_aggregate_expectation`, \
    and FileDataset implements the expectation methods themselves.
    """

    def __init__(self, *args, **kwargs):
        super(MetaFileDataset, self).__init__(*args, **kwargs)
        
        
    @classmethod
    
    def file_map_expectation(cls, func):
        """Constructs an expectation using file-map semantics.
        
        """
        if PY3:
            argspec = inspect.getfullargspec(func)[0][1:]
        else:
            argspec = inspect.getargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, mostly=None, skip=None, result_format=None, *args, **kwargs):

            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)
            
            lines=self.f.readlines() #Read in file lines



            
            
            #Skip k initial lines designated by the user
            if skip is not None:
                try:
                    assert float(skip).is_integer()
                    assert float(skip) >= 0
                except:
                    raise ValueError("skip must be a positive integer")
                    
                for i in range(1,skip+1):
                    lines.pop(0)
                    
            null_lines = re.compile("\s+") #Ignore lines with just white space
            boolean_mapped_null_lines=np.array([bool(null_lines.match(line)) for line in lines])
            

            element_count = int(len(lines))


            nonnull_lines = list(compress(lines,np.invert(boolean_mapped_null_lines)))
            nonnull_count = int((boolean_mapped_null_lines==False).sum())

            boolean_mapped_success_lines = np.array(func(self, nonnull_lines, *args, **kwargs))
            success_count = np.count_nonzero(boolean_mapped_success_lines)


            unexpected_list = list(compress(nonnull_lines,np.invert(boolean_mapped_success_lines)))
            nonnull_lines_index=list(xrange(len(nonnull_lines)))
            unexpected_index_list = list(compress(nonnull_lines_index,np.invert(boolean_mapped_success_lines)))

            #success, percent_success = self._calc_map_expectation_success(success_count, nonnull_count, mostly)
            
            
            if nonnull_count > 0:
                
                percent_success = success_count / nonnull_count
    
                if mostly != None:
                    success = bool(percent_success >= mostly)
    
                else:
                    success = bool(nonnull_count-success_count == 0)
    
            else:
                success = True
                percent_success = None
                
    
                return_obj = self._format_column_map_output(
                    result_format, success,
                    element_count, nonnull_count,
                    unexpected_list, unexpected_index_list
                )
    
    
    
            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper
    
    
class FileDataset(MetaFileDataset):
    """
    FileDataset instantiates the great_expectations Expectations API as a subclass of a python file object.
    For the full API reference, please see :func:`Dataset <great_expectations.Dataset.base.Dataset>`
    """


    def __init__(self, file_object, *args, **kwargs):
        super(FileDataset, self).__init__(*args, **kwargs)
        self.discard_subset_failing_expectations = kwargs.get('discard_subset_failing_expectations', False)
        self.f=file_object
        
        
        
        
#    @DocInherit
    @MetaFileDataset.file_map_expectation
            
    def expect_file_line_regex_match_count_to_be_between(self,regex,lines=None, skip=None,
                                                         expected_min_count=0, expected_max_count=None,
                                                         mostly=None, result_format=None, include_config=False, 
                                                         catch_exceptions=None, meta=None):
        
        
        try:
            comp_regex=re.compile(regex)
        except:
            raise ValueError("Must enter valid regular expression for regex")
        
        if expected_min_count != None:
            try:
                assert float(expected_min_count).is_integer()
                assert float(expected_min_count)>=0
            except:
                raise ValueError("expected_min_count must be a non-negative integer or None")
                
        
        if expected_max_count != None:
            try:
                assert float(expected_max_count).is_integer()
                assert float(expected_max_count)>=0
            except:
                raise ValueError("expected_max_count must be a non-negative integer or None")
                
        if expected_max_count !=None and expected_min_count != None:
            try:
                assert (expected_max_count >= expected_min_count)
            except:
                raise ValueError("expected_max_count must be greater than or equal to expected_min_count")
         
        
        if expected_max_count!=None and expected_min_count!=None: 
            truth_list=[True if(comp_regex.findall(line)>= expected_min_count and \
                                comp_regex.findall(line) <= expected_max_count) else False \
                                for line in lines]
        
        elif(expected_max_count!=None):
            truth_list=[True if(comp_regex.findall(line)>= expected_min_count) else False \
                                for line in lines]
            
        elif(expected_min_count!=None):
              truth_list=[True if(comp_regex.findall(line) <= expected_max_count) else False \
                                for line in lines]
        else:
            truth_list=[True for line in lines]
            
        return truth_list
            
          
#    @DocInherit
    @MetaFileDataset.file_map_expectation
    def expect_file_line_regex_match_count_to_equal(self,regex, lines=None,expected_count=0, skip=None,
                                                    mostly=None, result_format=None, 
                                                    include_config=False,catch_exceptions=None,meta=None):
        
        try:
            comp_regex=re.compile(regex)
        except:
            raise ValueError("Must enter valid regular expression for regex")
            
        try:
            assert float(expected_count).is_integer()
            assert float(expected_count)>=0
        
        except:
            raise ValueError("expected_count must be a non-negative integer")
            
        return [True if(comp_regex.findall(line) == expected_count) else False \
                                for line in lines]
    
    
    
    
        
    
    
    
