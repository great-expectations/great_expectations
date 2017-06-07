import json
import hashlib
import datetime
import numpy as np

from nose.tools import *
import great_expectations as ge

def test_DotDict():

    D = ge.util.DotDict({
        'x' : [1,2,4],
        'y' : [1,2,5],
        'z' : ['hello', 'jello', 'mello'],
    })

    D = ge.util.DotDict({
        "dataset_name" : None,
        "expectations" : []
    })
