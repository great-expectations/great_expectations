import json
import hashlib
import datetime
import numpy as np
import unittest

import great_expectations as ge

class TestUtilMethods(unittest.TestCase):

    def test_DotDict(self):

        D = ge.util.DotDict({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })

        D = ge.util.DotDict({
            "dataset_name" : None,
            "expectations" : []
        })
        #TODO: Make this a meaningful test
        self.assertEqual(1,1)

    def test_partition_helpers(self):
        #TODO Make a meaningful test
        self.assertEqual(1,1)

    def test_ensure_json_serializable(self):
        # TODO: Make a meaningful test
        self.assertEqual(1,1)
