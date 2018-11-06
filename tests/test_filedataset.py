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
    


    
    
    
    



    
    

    

            