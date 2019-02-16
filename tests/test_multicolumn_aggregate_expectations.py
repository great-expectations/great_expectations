#multi_col aggregate tests, use automobile dataset
from __future__ import division
import pytest
import pandas as pd
import great_expectations as ge
from .test_utils import assertDeepAlmostEqual

def test_expect_kl_divergence_between_columns_to_be_between():
    test_df = pd.read_csv("./tests/test_sets/car_data.csv") #Read in dataset to test kl_divergence functionality
    column_list=["city_mpg", "highway_mpg", "num_doors", "engine_location", "price"] # Create column subset list
    bins={"city_mpg":[0, 10, 20, 30, 40, 50],
          "highway_mpg":[0, 10, 20, 30, 40, 50]} #Set up bins for numeric variables

    test_df = ge.dataset.PandasDataset(test_df) #Create pandas dataset object

    with pytest.raises(ValueError): #Raise error when min and max are not specified
        test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                   expected_min = None,
                                                                   expected_max = None)
    with pytest.raises(ValueError): #Raise error when min and max are not numeric
        test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                   expected_min = "0",
                                                                   expected_max = "2")

    with pytest.raises(ValueError): #Raise error when min is more that max
        test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                   expected_min = 4,
                                                                   expected_max = 2)
 
    with pytest.raises(ValueError): #Raise error when there are no columns being examined
        test_df.expect_kl_divergence_between_columns_to_be_between([],
                                                                   expected_min = 0,
                                                                   expected_max = 3)

    #Test different Result Formats
    result_object_boolean = test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                                       expected_min = 0,
                                                                                       expected_max = 0.5,
                                                                                       bins = bins,
                                                                                       ignore_row_if = "any_value_is_missing",
                                                                                       result_format = "BOOLEAN_ONLY")
    expected_result_object_boolean = {"success":False}
    assertDeepAlmostEqual(expected_result_object_boolean, result_object_boolean)

    result_object_basic = test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                                     expected_min = 0,
                                                                                     expected_max = 0.5,
                                                                                     bins = bins,
                                                                                     ignore_row_if = "any_value_is_missing",
                                                                                     result_format = "BASIC")    
    expected_result_object_basic = {"success":False,
                                    "result":{"element_count":205,
                                              "missing_count": 6,
                                              "missing_percent":6/205,
                                              "column_count": 5,
                                              "evaluation_count":2,
                                              "unexpected_evaluation_count":1,
                                              "unexpected_evaluation_percent":0.5}}
    
    assertDeepAlmostEqual(expected_result_object_basic, result_object_basic)

    result_object_summary = test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                                       expected_min = 0,
                                                                                       expected_max = 0.5,
                                                                                       bins = bins,
                                                                                       ignore_row_if = "any_value_is_missing",
                                                                                       result_format = "SUMMARY")    
    
    expected_result_object_summary = {"success":False,
                                      "result":{"element_count":205,
                                                "missing_count": 6,
                                                "missing_percent":6/205,
                                                "column_count": 5,
                                                "evaluation_count":2,
                                                "unexpected_evaluation_count":1,
                                                "unexpected_evaluation_percent":0.5,
                                                "partial_unexpected_eval_list":{"num_doors v. engine_location": 1.13749285}
                                                }}  
    
    assertDeepAlmostEqual(expected_result_object_summary, result_object_summary)

    result_object_complete=test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                   expected_min = 0,
                                                                   expected_max = 0.5,
                                                                   bins = bins,
                                                                   ignore_row_if = "any_value_is_missing",
                                                                   result_format = "COMPLETE")

    expected_result_object_complete = {"success":False,
                                       "result":{"element_count":205,
                                                 "missing_count": 6,
                                                 "missing_percent":6/205,
                                                 "column_count": 5,
                                                 "evaluation_count":2,
                                                 "unexpected_evaluation_count":1,
                                                 "unexpected_evaluation_percent":0.5,
                                                 "partial_unexpected_eval_list":{"num_doors v. engine_location": 1.13749285},
                                                 'unexpected_eval_list':{"num_doors v. engine_location": 1.13749285}}}
    
    assertDeepAlmostEqual(expected_result_object_complete, result_object_complete)

    #Successful Case
    result_object_successful=test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                                        expected_min = 0,
                                                                                        expected_max = 1.5,
                                                                                        bins = bins,
                                                                                        ignore_row_if = "any_value_is_missing")
    assert result_object_successful["success"]

    #Ignore if all are null instead of only some
    result_object_allmissing=test_df.expect_kl_divergence_between_columns_to_be_between(column_list,
                                                                                        expected_min = 0,
                                                                                        expected_max = 0.5,
                                                                                        bins = bins,
                                                                                        ignore_row_if = "all_values_are_missing",
                                                                                        result_format = "COMPLETE")
    expected_result_object_allmissing = {"success":False,
                                         "result":{"element_count":205,
                                                   "missing_count": 0,
                                                   "missing_percent":0,
                                                   "column_count": 5,
                                                   "evaluation_count":2,
                                                   "unexpected_evaluation_count":1,
                                                   "unexpected_evaluation_percent":0.5,
                                                   "partial_unexpected_eval_list":{"num_doors v. engine_location": 1.17481015},
                                                   'unexpected_eval_list':{"num_doors v. engine_location": 1.17481015}}} 

    assertDeepAlmostEqual(expected_result_object_allmissing, result_object_allmissing)
    
    
    
    
    
       
                                                                                                           
    
    
    
        


        
    
        
    
        
    
    
    
    
    
    
    
    