import json
import hashlib
import datetime
import numpy as np

from nose.tools import *
import great_expectations as ge

def test_dataset():

    D = ge.dataset.PandasDataSet({
        'x' : [1,2,4],
        'y' : [1,2,5],
        'z' : ['hello', 'jello', 'mello'],
    })

    # print D._expectations_config.keys()
    # print json.dumps(D._expectations_config, indent=2)
    
    assert_equal(
        D._expectations_config,
        {
            "dataset_name" : None,
            "expectations" : [{
                "expectation_type" : "expect_column_to_exist",
                "kwargs" : { "column" : "x" }
            },{
                "expectation_type" : "expect_column_to_exist",
                "kwargs" : { "column" : "y" }
            },{
                "expectation_type" : "expect_column_to_exist",
                "kwargs" : { "column" : "z" }
            }]
        }
    )

    assert_equal(
        D.get_expectations_config(),
        {
            "dataset_name" : None,
            "expectations" : [{
                "expectation_type" : "expect_column_to_exist",
                "kwargs" : { "column" : "x" }
            },{
                "expectation_type" : "expect_column_to_exist",
                "kwargs" : { "column" : "y" }
            },{
                "expectation_type" : "expect_column_to_exist",
                "kwargs" : { "column" : "z" }
            }]
        }
    )


    #!!! Add tests for expectation and column_expectation
    #!!! Add tests for save_expectation
