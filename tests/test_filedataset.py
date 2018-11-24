# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 15:23:27 2018

@author: anhol
"""
from __future__ import division

import pytest
import great_expectations as ge
import great_expectations.dataset.autoinspect as autoinspect
import warnings
from .test_utils import assertDeepAlmostEqual

def test_autoinspect_filedataset():
    #Expect a warning to be raised since a file object doesn't have a columns attribute
    
    warnings.simplefilter('always', UserWarning)
    file_path='./tests/test_sets/toy_data_complete.csv'
    
    my_file_data=ge.dataset.FileDataset(file_path)
    
    with pytest.raises(UserWarning):
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("error")
            try:
                my_file_data.autoinspect(autoinspect.columns_exist)
            except:
                raise
    
    
    
def test_expectation_config_filedataset():
   
    #Load in data files
    file_path='./tests/test_sets/toy_data_complete.csv'
    
    #Create FileDataset objects
    
    f_dat=ge.dataset.FileDataset(file_path)
    
    
    #Set up expectations
    f_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                            expected_count=3,
                                                            skip=1,result_format="BASIC",
                                                            catch_exceptions=True) 
    
    
    f_dat.expect_file_line_regex_match_count_to_be_between(regex=',\S',
                                                            expected_max_count=2,
                                                            skip=1,result_format="SUMMARY",
                                                            include_config=True) 
    
    
    
    #Test basic config output
    complete_config=f_dat.get_expectations_config()
    
    expected_config_expectations=[{'expectation_type': 'expect_file_line_regex_match_count_to_equal',
                                       'kwargs': {'expected_count': 3,
                                                  'regex': ',\\S',
                                                  "skip":1}}]
    
    assertDeepAlmostEqual(complete_config["expectations"],expected_config_expectations)
    
    
    #Include result format kwargs
    complete_config2=f_dat.get_expectations_config(discard_result_format_kwargs=False,
                                                               discard_failed_expectations=False)
    
    expected_config_expectations2=[{'expectation_type': 'expect_file_line_regex_match_count_to_equal',
                                       'kwargs': {'expected_count': 3,
                                                  'regex': ',\\S',
                                                  "result_format": "BASIC",
                                                  "skip":1}},
                                      {'expectation_type':'expect_file_line_regex_match_count_to_be_between',
                                       'kwargs':{'expected_max_count':2,
                                                "regex":",\\S",
                                                "result_format":"SUMMARY",
                                                "skip":1}}]
    
    
    assertDeepAlmostEqual(complete_config2["expectations"],expected_config_expectations2)
    
    
    
    #Discard Failing Expectations
    
    complete_config3=f_dat.get_expectations_config(discard_result_format_kwargs=False,
                                                               discard_failed_expectations=True)
    
    expected_config_expectations3=[{'expectation_type': 'expect_file_line_regex_match_count_to_equal',
                                       'kwargs': {'expected_count': 3,
                                                  'regex': ',\\S',
                                                  "result_format": "BASIC",
                                                  "skip":1}}]
    
    assertDeepAlmostEqual(complete_config3["expectations"],expected_config_expectations3)
    
    
    


    
#Raise NotImplementedError when using FileDataset expectations from Dataset object
def test_filedataset_expectations_NotImplementedError():
   
    #Create Dataset objects
    
    
    f_dat=ge.dataset.DataFile()
    
    #Test expect_file_line_regex+match_count_to_equal
    
    with pytest.raises(NotImplementedError):
        
        try:
            f_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                            expected_count=3,
                                                            skip=1,result_format="BASIC")
        except:
            raise
   
    #Test expect_file_line_regex+match_count_to_between
    with pytest.raises(NotImplementedError):
        
        try:
            f_dat.expect_file_line_regex_match_count_to_be_between(regex=',\S',
                                                            expected_max_count=3,
                                                            skip=1,result_format="BASIC")
        except:
            raise
            
            
def test_file_format_map_output():
    incomplete_file_path='./tests/test_sets/toy_data_incomplete.csv'
    incomplete_file_dat=ge.dataset.FileDataset(incomplete_file_path) 
    
    null_file_path='./tests/test_sets/null_file.csv'
    null_file_dat=ge.dataset.FileDataset(null_file_path) 
    
    white_space_path='./tests/test_sets/white_space.txt'
    white_space_dat=ge.dataset.FileDataset(white_space_path) 
    
    
    #Boolean Expectation Output
    expectation=incomplete_file_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                                                expected_count=3,
                                                                                skip=1,
                                                                                result_format="BOOLEAN_ONLY")
    expected_result={"success":False}
    
    assertDeepAlmostEqual(expected_result, expectation)
    
    
    #Empty File Expectations
    expectation=null_file_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                                                expected_count=3,
                                                                                skip=1,
                                                                                result_format="BASIC")
    
    expected_result={"success":None,
                     "result":{"element_count":0, "missing_count":0,
                               "missing_percent":None, "unexpected_count":0,
                               "unexpected_percent":None,"unexpected_percent_nonmissing":None,
                               "partial_unexpected_list":[]
                             }
                         }
                     
    assertDeepAlmostEqual(expected_result,expectation)
    
    
    #White Space File
    
    expectation=white_space_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                                                expected_count=3,
                                                                                result_format="BASIC")
    
    expected_result={"success":None,
                     "result":{"element_count":11, "missing_count":11,
                               "missing_percent":1, "unexpected_count":0,
                               "unexpected_percent":0,"unexpected_percent_nonmissing":None,
                               "partial_unexpected_list":[]
                             }
                         }
                     
    assertDeepAlmostEqual(expected_result,expectation)
    
    
    
    #Complete Result Format
    expectation=incomplete_file_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                                                expected_count=3,
                                                                                skip=1,
                                                                                result_format="COMPLETE")
    
    
    expected_result={"success":False,
                     "result":{"element_count":9, "missing_count":2,
                               "missing_percent":2/9, "unexpected_count":3,
                               "unexpected_percent":3/9,"unexpected_percent_nonmissing":3/7,
                               "partial_unexpected_list":['A,C,1\n', 'B,1,4\n', 'A,1,4\n'],
                               "partial_unexpected_counts":[{"value":'A,1,4\n',"count":1},
                                                            {"value":'A,C,1\n',"count":1},
                                                            {"value":'B,1,4\n',"count":1}],
                               "partial_unexpected_index_list":[0,3,5],
                               "unexpected_list":['A,C,1\n', 'B,1,4\n', 'A,1,4\n'],
                               "unexpected_index_list":[0,3,5]
                             }
                         }
    
    assertDeepAlmostEqual(expected_result,expectation)
    
    
    #Invalid Result Format
    with pytest.raises(ValueError):
            expectation=incomplete_file_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                                                expected_count=3,
                                                                                skip=1,
                                                                                result_format="JOKE")
            

        
    
    
    
    
    
    
                
    
    



    
    

    

            