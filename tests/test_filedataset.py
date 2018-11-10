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
    f=open('./tests/test_sets/toy_data_complete.csv',"r")
    
    my_file_data=ge.dataset.FileDataset(f)
    
    with pytest.raises(UserWarning):
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("error")
            try:
                my_file_data.autoinspect(autoinspect.columns_exist)
            except:
                raise
    
    f.close()
    
    
def test_expectation_config_filedataset():
   
    #Load in data files
    f=open('./tests/test_sets/toy_data_complete.csv',"r")
    
    #Create FileDataset objects
    
    f_dat=ge.dataset.FileDataset(f)
    
    
    #Set up expectations
    f_dat.expect_file_line_regex_match_count_to_equal(regex=',\S',
                                                            expected_count=3,
                                                            skip=1,result_format="BASIC") 
    
    
    f_dat.expect_file_line_regex_match_count_to_be_between(regex=',\S',
                                                            expected_max_count=2,
                                                            skip=1,result_format="SUMMARY") 
    
    
    
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
    
    f.close()
    


    
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
                
    
    



    
    

    

            