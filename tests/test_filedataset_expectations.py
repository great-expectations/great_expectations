# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 12:09:54 2018

@author: anhol
"""

#Test File Expectations
from __future__ import division
import great_expectations as ge
import pytest



def test_expect_file_line_regex_match_count_to_be_between():
    
    complete_file_path='./tests/test_sets/toy_data_complete.csv'
    
    file_dat=ge.dataset.FileDataset(complete_file_path)
    
    
    #Invalid Skip Parameter
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                            expected_min_count=0, 
                                                            expected_max_count=4,
                                                            skip=2.4) 

    #Invalid Regex
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(regex=2,
                                                            expected_min_count=1, 
                                                            expected_max_count=8,
                                                            skip=2) 
    
        
    
    #Non-integer min value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                            expected_min_count=1.3, 
                                                            expected_max_count=8,
                                                            skip=1) 

    
    #Negative min value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                                     expected_min_count=-2,
                                                                     expected_max_count=8,
                                                                     skip=1)



    #Non-integer max value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                            expected_min_count=0,
                                                            expected_max_count="foo",
                                                            skip=1) 
        
    #Negative max value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                            expected_min_count=0,
                                                            expected_max_count=-1,
                                                            skip=1) 

    
    #Min count more than max count
    
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                            expected_min_count=4,
                                                            expected_max_count=3,
                                                            skip=1) 

    #Count does not fall in range
    fail_trial=file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                                         expected_min_count=9,
                                                                         expected_max_count=12,
                                                                         skip=1)
    
    assert (not fail_trial["success"])
    assert fail_trial['result']['unexpected_percent']==1
    assert fail_trial['result']['missing_percent']==0
    
    #Count does fall in range
    success_trial=file_dat.expect_file_line_regex_match_count_to_be_between(regex=",\S",
                                                                        expected_min_count=0,
                                                                        expected_max_count=4,
                                                                        skip=1)
    assert success_trial["success"]
    assert success_trial['result']['unexpected_percent']==0
    assert success_trial['result']['missing_percent']==0
    
    
    
    file_dat.close()
    
    
  
    
def test_expect_file_line_regex_match_count_to_equal():
    
    complete_file_path='./tests/test_sets/toy_data_complete.csv'
    incomplete_file_path='./tests/test_sets/toy_data_incomplete.csv'
    
    file_dat=ge.dataset.FileDataset(complete_file_path)
    file_incomplete_dat=ge.dataset.FileDataset(incomplete_file_path)
    
    #Invalid Regex Value
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_equal(regex=True,
                                                            expected_count=3,
                                                            skip=1) 
    
    
    #Non-integer expected_count
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_equal(regex=",\S",
                                                            expected_count=6.3,
                                                            skip=1) 

    
    #Negative expected_count
    with pytest.raises(ValueError):
        file_dat.expect_file_line_regex_match_count_to_equal(regex=",\S",
                                                                   expected_count=-6,
                                                                   skip=1)
        
    

    #Count does not equal expected count
    fail_trial=file_incomplete_dat.expect_file_line_regex_match_count_to_equal(regex=",\S",
                                                                    expected_count=3,
                                                                    skip=1)
    
    assert (not fail_trial["success"])
    assert fail_trial['result']['unexpected_percent']==3/9
    assert fail_trial['result']['missing_percent']==2/9
    assert fail_trial['result']['unexpected_percent_nonmissing']==3/7
    
    
    #Mostly success
    mostly_trial=file_incomplete_dat.expect_file_line_regex_match_count_to_equal(regex=",\S",
                                                                    expected_count=3,
                                                                    skip=1,
                                                                    mostly=0.57)
    
    assert mostly_trial["success"]
    
    
    #Count does fall in range
    success_trial=file_dat.expect_file_line_regex_match_count_to_equal(regex=",\S",
                                                                            expected_count=3,
                                                                            skip=1)
    assert success_trial["success"]
    assert success_trial['result']['unexpected_percent']==0
    assert success_trial['result']['unexpected_percent_nonmissing']==0
    assert success_trial['result']['missing_percent']==0
    
    
    
    file_dat.close()
    file_incomplete_dat.close()

    
    


