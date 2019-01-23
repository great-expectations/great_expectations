from __future__ import division

from .base import DataFile, Dataset
import re
import numpy as np
from itertools import compress
from six import PY3
from .util import DocInherit, parse_result_format
import inspect
from functools import wraps
import hashlib
import os
import csv
import json
import jsonschema


class MetaFileDataset(DataFile):
    """MetaFileDataset is a thin layer between Dataset and FileDataset.
    This two-layer inheritance is required to make @classmethod decorators work.
    Practically speaking, that means that MetaFileDataset implements \
    expectation decorators, like `file_lines_map_expectation` \
    and FileDataset implements the expectation methods themselves.
    """

    def __init__(self, *args, **kwargs):
        super(MetaFileDataset, self).__init__(*args, **kwargs)
        
        
    @classmethod
    
    def file_lines_map_expectation(cls, func):
        """Constructs an expectation using file lines map semantics.
        
        """
        if PY3:
            argspec = inspect.getfullargspec(func)[0][1:]
        else:
            argspec = inspect.getargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, mostly=None, skip=None, result_format=None, *args, **kwargs):

            try: 
                f=open(self.path,"r")
            except:
                raise 
            
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)
            
            lines=f.readlines() #Read in file lines


            
            
            #Skip k initial lines designated by the user
            if skip is not None and skip <= len(lines):
                try:
                    assert float(skip).is_integer()
                    assert float(skip) >= 0
                except:
                    raise ValueError("skip must be a positive integer")
                    
                for i in range(1,skip+1):
                    lines.pop(0)
                    
            
            if(len(lines)>0):
                
                null_lines = re.compile("\s+") #Ignore lines with just white space
                boolean_mapped_null_lines=np.array([bool(null_lines.match(line)) for line in lines])
                element_count = int(len(lines))
                
                if(element_count > sum(boolean_mapped_null_lines)):
                    
                    nonnull_lines = list(compress(lines,np.invert(boolean_mapped_null_lines)))
                    nonnull_count = int((boolean_mapped_null_lines==False).sum())
                    boolean_mapped_success_lines = np.array(func(self, lines=nonnull_lines, *args, **kwargs))
                    success_count = np.count_nonzero(boolean_mapped_success_lines)
                    
                    unexpected_list = list(compress(nonnull_lines,np.invert(boolean_mapped_success_lines)))
                    nonnull_lines_index=range(0, len(nonnull_lines)+1)
                    unexpected_index_list = list(compress(nonnull_lines_index,np.invert(boolean_mapped_success_lines)))
                    
                    success, percent_success = self._calc_map_expectation_success(success_count, nonnull_count, mostly)
            
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
                return_obj = self._format_map_output(
                        result_format=result_format, success=None,
                        element_count=0, nonnull_count=0,
                        unexpected_list=[], unexpected_index_list=[]
                    )
            
                    
            
            f.close()
            
            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper
    
    
class FileDataset(MetaFileDataset):
    """
    FileDataset instantiates the great_expectations Expectations API as a subclass of a python file object.
    For the full API reference, please see :func:`Dataset <great_expectations.Dataset.base.Dataset>`
    """


    def __init__(self, file_path, *args, **kwargs):
        super(FileDataset, self).__init__(*args, **kwargs)
        self.path=file_path
        
        
        
        
    @DocInherit
    @MetaFileDataset.file_lines_map_expectation
            
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
            truth_list=[True if(len(comp_regex.findall(line))>= expected_min_count and \
                                len(comp_regex.findall(line)) <= expected_max_count) else False \
                                for line in lines]
        
        elif(expected_max_count!=None):
            truth_list=[True if(len(comp_regex.findall(line))<= expected_max_count) else False \
                                for line in lines]
            
        elif(expected_min_count!=None):
              truth_list=[True if(len(comp_regex.findall(line)) >= expected_min_count) else False \
                                for line in lines]
        else:
            truth_list=[True for line in lines]
            
        return truth_list
            
          
    @DocInherit
    @MetaFileDataset.file_lines_map_expectation
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
            
        return [True if(len(comp_regex.findall(line)) == expected_count) else False \
                                for line in lines]
    
    
    
    @DocInherit
    @Dataset.expectation(["value"])
    
    def expect_file_hash_to_equal(self, value, hash_alg='md5', result_format=None, 
                                                    include_config=False,catch_exceptions=None,meta=None):
        success = False
        try:
            hash = hashlib.new(hash_alg)
        
        # Limit file reads to 64 KB chunks at a time
            BLOCKSIZE = 65536
            try:
                with open(self.path, 'rb') as file:
                    file_buffer = file.read(BLOCKSIZE)
                    while len(file_buffer) > 0:
                        hash.update(file_buffer)
                        file_buffer = file.read(BLOCKSIZE)
                    success = hash.hexdigest() == value
            except IOError:
                raise
        except ValueError:
            raise
        return {"success":success}
    
    
    @DocInherit
    @Dataset.expectation(["minsize","maxsize"])
    def expect_file_size_to_be_between(self, minsize, maxsize,result_format=None, 
                                                    include_config=False,catch_exceptions=None,
                                                    meta=None):
        
       
        
        success=False
        try:
            size = os.path.getsize(self.path)
        except OSError:
            raise
            
            
        if type(minsize) != int:
            raise TypeError('minsize must be an integer')
            
        if type(maxsize) != int:
            raise TypeError('maxsize must be an integer')
            
        if minsize < 0:
            raise ValueError('minsize must be greater than of equal to 0')
                
        if maxsize < 0:
            raise ValueError('maxsize must be greater than of equal to 0')
            
        if minsize > maxsize:
            raise ValueError('maxsize must be greater than of equal to minsize')
            
        if (size >= minsize) and (size <= maxsize):
            success=True
           
        return {"success":success}
    
        
    @DocInherit
    @Dataset.expectation([])   
    def expect_file_to_exist(self,result_format=None,include_config=False,
                             catch_exceptions=None, meta=None):
        
        success=False
        if os.path.isfile(self.path):
            success=True
            
        return {"success":success}
    
    
    @DocInherit
    @Dataset.expectation([])
    def expect_file_to_have_valid_table_header(self,regex,skip=None,
                                           result_format=None, 
                                           include_config=False,
                                           catch_exceptions=None,meta=None):
        
        try:
            comp_regex=re.compile(regex)
        except:
            raise ValueError("Must enter valid regular expression for regex")
        
        success = False
        
        try:
            with open(self.path, 'r') as f:
                lines=f.readlines() #Read in file lines
        
        except IOError:
            raise

            #Skip k initial lines designated by the user
        if skip is not None and skip <= len(lines):
            try:
                assert float(skip).is_integer()
                assert float(skip) >= 0
            except:
                raise ValueError("skip must be a positive integer")
                    
            for i in range(1,skip+1):
                lines.pop(0)
                    
        header_line = lines[0].strip()
        header_names=comp_regex.split(header_line)
        if len(set(header_names)) == len(header_names):
            success = True

        return {"success":success}
    
    
    @DocInherit
    @Dataset.expectation([])
    def expect_file_to_be_valid_json(self, schema=None,result_format=None,
                               include_config=False,catch_exceptions=None, 
                               meta=None ):
        
        success = False
        if schema is None:
            try:
                with open(self.path, 'r') as f:
                    json.load(f)
                success = True
            except ValueError:
                success = False
        else:
            try:
                with open(schema, 'r') as s:
                    schema_data = s.read()
                sdata = json.loads(schema_data)
                with open(self.path, 'r') as f:
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
        return {"success":success}
    
    
    
    
    
    
    
    

        
    
    
    
