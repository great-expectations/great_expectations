from __future__ import division

import unittest
import json
import numpy as np
import datetime

import great_expectations as ge


class TestPandasDataset(unittest.TestCase):

    # def test_expect_column_to_exist(self):
    #     print("=== test_expect_column_to_exist ===")
    #     with open("./tests/test_sets/expect_column_to_exist_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         print(t)
    #         out = D.expect_column_to_exist(**t['in'])

    #         if 'out' in t:
    #             self.assertEqual(out, t['out'])

    #         if 'error' in t:
    #             self.assertEqual(out['raised_exception'], True)
    #             self.assertIn(t['error']['traceback_substring'], out['exception_traceback'])

    def test_expect_column_values_to_be_unique(self):

        D = ge.dataset.PandasDataSet({
            'a' : ['2', '2'],
            'b' : [1, '2'],
            'c' : [1, 1],
            'd' : [1, '1'],
            'n' : [None, np.nan]
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        # Tests for D
        T = [
                {
                    'in':{'column':'a'},
                    'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':['2','2']}},
                {
                    'in':{'column':'b'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'c'},
                    'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':[1,1]}},
                {
                    'in':{'column':'d'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'n'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_unique(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        df = ge.dataset.PandasDataSet({
            'a' : ['2', '2', '2', '2'],
            'b' : [1, '2', '2', '3'],
            'n' : [None, None, np.nan, None],
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        # Tests for df
        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2,3], 'unexpected_list':['2','2','2','2']}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.25},
                    'out':{'success':True, 'unexpected_index_list':[1,2], 'unexpected_list':['2','2']}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.75},
                    'out':{'success':False, 'unexpected_index_list':[1,2], 'unexpected_list':['2','2']}},
                {
                    'in':['a'],
                    'kwargs':{'mostly':1},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2,3], 'unexpected_list':['2','2','2','2']}},
                {
                    'in':['n'],
                    'kwargs':{'mostly':.2},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}}
        ]

        for t in T:
            out = df.expect_column_values_to_be_unique(*t['in'], **t['kwargs'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])


    def test_expect_column_values_to_not_be_null(self):
        """
        Cases Tested:
            F: Column with one None value and other non None value
            F: Column with one np.nan value and other non np.nan value
            F: Column with one np.nan value and None value
            T: Column with non None or np.nan
        """

        D = ge.dataset.PandasDataSet({
            'x' : [2, None],
            'y' : [2, np.nan],
            'n' : [None, np.nan],
            'z' : [2, 5],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'y'},
                    'out':{'success':False, 'unexpected_index_list':[1], 'unexpected_list':[None]}},
                {
                    'in':{'column':'n'},
                    'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':[None, None]}},
                # {
                #     'in':{'column':'y'},
                #     'out':{'success':False, 'unexpected_index_list':[1], 'unexpected_list':[np.nan]}},
                # {
                #     'in':{'column':'n'},
                #     'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':[None, np.nan]}},
                {
                    'in':{'column':'x'},
                    'out':{'success':False, 'unexpected_index_list':[1], 'unexpected_list':[None]}},
                {
                    'in':{'column':'z'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}}
        ]


        for t in T:
            out = D.expect_column_values_to_not_be_null(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        D2 = ge.dataset.PandasDataSet({
            'a' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'b' : [1, 2, 3, 4, 5, 6, 7, 8, 9, None],
        })
        D2.set_default_expectation_argument("result_format", "COMPLETE")

        #assert_equal(
        #    D.expect_column_values_to_not_be_null('x'),
        #    {'success':False, 'unexpected_list':[None]}
        #)

        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':['a'],
                    'kwargs':{'mostly':.90},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':['b'],
                    'kwargs':{},
                    'out':{'success':False, 'unexpected_index_list':[9], 'unexpected_list':[None]}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.95},
                    'out':{'success':False, 'unexpected_index_list':[9], 'unexpected_list':[None]}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.90},
                    'out':{'success':True, 'unexpected_index_list':[9], 'unexpected_list':[None]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        D3 = ge.dataset.PandasDataSet({
            'a' : [None, None, None, None],
        })
        D3.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2,3], 'unexpected_list':[None,None,None,None]}
                },
                {
                    'in':['a'],
                    'kwargs':{"mostly":.95},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2,3], 'unexpected_list':[None,None,None,None]}
                },
        ]

        for t in T:
            out = D3.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            # out = D3.expect_column_values_to_be_null(*t['in'], **t['kwargs'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_values_to_be_null(self):
        """
        !!! All values must be either None and np.nan to be True
        Cases Tested:
            F: Column with one None value and other non None value
            F: Column with one np.nan value and other non np.nan value
            F: Column with one np.nan value and None value
            T: Column with non None or np.nan values
        """

        D = ge.dataset.PandasDataSet({
            'x' : [2, None, 2],
            'y' : [2, np.nan, 2],
            'z' : [2, 5, 7],
            'a' : [None, np.nan, None],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x'},
                    'out':{'success':False, 'unexpected_index_list':[0,2], 'unexpected_list':[2,2]}},
                {
                    'in':{'column':'y'},
                    'out':{'success':False, 'unexpected_index_list':[0,2], 'unexpected_list':[2,2]}},
                {
                    'in':{'column':'z'},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2], 'unexpected_list':[2,5,7]}},
                {
                    'in':{'column':'a'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'x', 'mostly':.2},
                    'out':{'success':True, 'unexpected_index_list':[0,2], 'unexpected_list':[2,2]}},
                {
                    'in':{'column':'x', 'mostly':.8},
                    'out':{'success':False, 'unexpected_index_list':[0,2], 'unexpected_list':[2,2]}
                    },
                {
                    'in':{'column':'a', 'mostly':.5},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_null(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        D3 = ge.dataset.PandasDataSet({
            'a' : [None, None, None, None],
            'b' : [np.nan, np.nan, np.nan, np.nan],
        })
        D3.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}
                },
                {
                    'in':['a'],
                    'kwargs':{"mostly":.95},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}
                },
                {
                    'in':['b'],
                    'kwargs':{},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}
                },
                {
                    'in':['b'],
                    'kwargs':{"mostly":.95},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}
                },
        ]

        for t in T:
            # out = D3.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            out = D3.expect_column_values_to_be_null(*t['in'], **t['kwargs'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_values_to_be_of_type(self):

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1.0,2.2,5.3],
            'z' : ['hello', 'jello', 'mello'],
            'n' : [None, np.nan, None],
            'b' : [False, True, False],
            's' : ['hello', 'jello', 1],
            's1' : ['hello', 2.0, 1],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{"column":"x","type_":"int","target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{"column":"x","type_":"string","target_datasource":"numpy"},
                    'out':{'success':False, 'unexpected_list':[1,2,4], 'unexpected_index_list':[0,1,2]}},
                {
                    'in':{"column":"y","type_":"float","target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{"column":"y","type_":"float","target_datasource":"numpy"},
                    'out':{'success':False, 'unexpected_list':[1.0,2.2,5.3], 'unexpected_index_list':[0,1,2]}},
                {
                    'in':{"column":"z","type_":"string","target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{"column":"b","type_":"boolean","target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}}
                #{
                #    'in':['n','null','python'],
                #    'kwargs':{},
                #    'out':{'success':False, 'unexpected_list':[np.nan]}},
                #{
                #    'in':['n','null','python'],
                #    'kwargs':{'mostly':.5},
                #    'out':{'success':True, 'unexpected_list':[np.nan]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_of_type(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_values_to_be_in_type_list(self):

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1.0,2.2,5.3],
            'z' : ['hello', 'jello', 'mello'],
            'n' : [None, np.nan, None],
            'b' : [False, True, False],
            's' : ['hello', 'jello', 1],
            's1' : ['hello', 2.0, 1],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{"column":"x","type_list":["int"],"target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{"column":"x","type_list":["string"],"target_datasource":"numpy"},
                    'out':{'success':False, 'unexpected_list':[1,2,4], 'unexpected_index_list':[0,1,2]}},
                {
                    'in':{"column":"y","type_list":["float"],"target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{"column":"y","type_list":["float"],"target_datasource":"numpy"},
                    'out':{'success':False, 'unexpected_list':[1.0,2.2,5.3], 'unexpected_index_list':[0,1,2]}},
                {
                    'in':{"column":"z","type_list":["string"],"target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{"column":"b","type_list":["boolean"],"target_datasource":"python"},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                   'in':{"column":"s", "type_list":["string", "int"], "target_datasource":"python"},
                   'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                #{
                #    'in':['n','null','python'],
                #    'kwargs':{'mostly':.5},
                #    'out':{'success':True, 'unexpected_list':[np.nan]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_in_type_list(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_values_to_be_in_set(self):
        """
        Cases Tested:

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':['x', [1,2,4]],
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':['x', [4,2]],
                    'out':{'success':False, 'unexpected_index_list':[0], 'unexpected_list':[1]}},
                {
                    'in':['y', []],
                    'out':{'success':False, 'unexpected_index_list':[0,1,2], 'unexpected_list':[1,2,5]}},
                {
                    'in':['z', ['hello','jello','mello']],
                    'out': {'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':['z', ['hello']],
                    'out': {'success':False, 'unexpected_index_list':[1,2], 'unexpected_list':['jello','mello']}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_in_set(*t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        self.assertRaises(
            TypeError,
            D.expect_column_values_to_be_in_set, 'x', None
        )

        D2 = ge.dataset.PandasDataSet({
            'x' : [1,1,2,None],
            'y' : [None,None,None,None],
        })
        D2.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x', 'values_set':[1,2]},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'x', 'values_set':[1]},
                    'out':{'success':False, 'unexpected_index_list':[2], 'unexpected_list':[2]}},
                {
                    'in':{'column':'x', 'values_set':[1], 'mostly':.66},
                    'out':{'success':True, 'unexpected_index_list':[2], 'unexpected_list':[2]}},
                {
                    'in':{'column':'x', 'values_set':[2], 'mostly':.66},
                    'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':[1,1]}},
                {
                    'in':{'column':'y', 'values_set':[]},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'y', 'values_set':[2], 'mostly':.5},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_be_in_set(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_values_to_not_be_in_set(self):
        """
        Cases Tested:
        -Repeat values being returned
        -Running expectations only on nonmissing values
        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'z' : ['hello', 'jello', 'mello'],
            'a' : [1,1,2],
            'n' : [None,None,2],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':['x', [1,2]],'kwargs':{},
                    'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':[1,2]}},
                {
                    'in':['x',[5,6]],'kwargs':{},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':['z',['hello', 'jello']],'kwargs':{},
                    'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':['hello', 'jello']}},
                {
                    'in':['z',[]],'kwargs':{},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':['a', [1]],'kwargs':{},
                    'out':{'success':False, 'unexpected_index_list':[0,1], 'unexpected_list':[1, 1]}},
                {
                    'in':['n', [2]],
                    'kwargs':{},
                    'out':{'success':False, 'unexpected_index_list':[2], 'unexpected_list':[2]}},
                {
                    'in':['n', []],
                    'kwargs':{},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':['a', [1]], 
                    'kwargs':{'mostly':.1},
                    'out':{'success':True, 'unexpected_index_list':[0,1], 'unexpected_list':[1, 1]}},
                {
                    'in':['n', [2]],
                    'kwargs':{'mostly':.9},
                    'out':{'success':False, 'unexpected_index_list':[2], 'unexpected_list':[2]}}
        ]

        for t in T:
            out = D.expect_column_values_to_not_be_in_set(*t['in'],**t['kwargs'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])


    # def test_expect_column_values_to_be_between(self):
    #     """

    #     """

    #     with open("./tests/test_sets/expect_column_values_to_be_between_test_set.json") as f:
    #         fixture = json.load(f)

    #     dataset = fixture["dataset"]
    #     tests = fixture["tests"]

    #     D = ge.dataset.PandasDataSet(dataset)
    #     D.set_default_expectation_argument("result_format", "COMPLETE")

    #     self.maxDiff = None

    #     for t in tests:
    #         out = D.expect_column_values_to_be_between(**t['in'])

    #         # print '-'*80
    #         print(t)
    #         # print(json.dumps(out, indent=2))

    #         if 'out' in t:
    #             self.assertEqual(t['out']['success'], out['success'])
    #             if 'unexpected_index_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #             if 'unexpected_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    #         if 'error' in t:
    #             self.assertEqual(out['exception_info']['raised_exception'], True)
    #             self.assertIn(t['error']['traceback_substring'], out['exception_info']['exception_traceback'])

    def test_expect_column_value_lengths_to_be_between(self):
        D = ge.dataset.PandasDataSet({
            's1':['smart','silly','sassy','slimy','sexy'],
            's2':['cool','calm','collected','casual','creepy'],
            's3':['cool','calm','collected','casual',None],
            's4':[1,2,3,4,5]
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'s1', 'min_value':3, 'max_value':5},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'s2', 'min_value':4, 'max_value':6},
                    'out':{'success':False, 'unexpected_index_list':[2], 'unexpected_list':['collected']}},
                {
                    'in':{'column':'s2', 'min_value':None, 'max_value':10},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'s3', 'min_value':None, 'max_value':10},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}}
        ]

        for t in T:
            out = D.expect_column_value_lengths_to_be_between(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])


        with self.assertRaises(TypeError):
            D.expect_column_value_lengths_to_be_between(**{'column':'s4', 'min_value':None, 'max_value':10})

        with self.assertRaises(ValueError):
            D.expect_column_value_lengths_to_be_between("s4", min_value=None, max_value=None)


    def test_expect_column_values_to_match_regex(self):
        """
        Cases Tested:
            Tested mostly alphabet regex
        """

        D = ge.dataset.PandasDataSet({
            'x' : ['aa', 'ab', 'ac', 'a1', None],
            'y' : ['aa', 'ab', 'ac', 'ba', 'ca'],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")


        D2 = ge.dataset.PandasDataSet({
            'a' : ['aaa', 'abb', 'acc', 'add', 'bee'],
            'b' : ['aaa', 'abb', 'acc', 'bdd', None],
            'c' : [ None,  None,  None,  None, None],
        })
        D2.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x', 'regex':'^a'},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{'column':'x', 'regex':'aa'},
                    'out':{'success':False, 'unexpected_list':['ab', 'ac', 'a1'], 'unexpected_index_list':[1,2,3]}},
                {
                    'in':{'column':'x', 'regex':'a[a-z]'},
                    'out':{'success':False, 'unexpected_list':['a1'], 'unexpected_index_list':[3]}},
                {
                    'in':{'column':'y', 'regex':'[abc]{2}'},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{'column':'y', 'regex':'[z]'},
                    'out':{'success':False, 'unexpected_list':['aa', 'ab', 'ac', 'ba', 'ca'], 'unexpected_index_list':[0,1,2,3,4]}}
        ]

        for t in T:
            out = D.expect_column_values_to_match_regex(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        T = [
                {
                    'in':{'column':'a', 'regex':'^a', 'mostly':.9},
                    'out':{'success':False, 'unexpected_list':['bee'], 'unexpected_index_list':[4]}},
                {
                    'in':{'column':'a', 'regex':'^a', 'mostly':.8},
                    'out':{'success':True, 'unexpected_list':['bee'], 'unexpected_index_list':[4]}},
                {
                    'in':{'column':'a', 'regex':'^a', 'mostly':.7},
                    'out':{'success':True, 'unexpected_list':['bee'], 'unexpected_index_list':[4]}},
                {
                    'in':{'column':'b', 'regex':'^a', 'mostly':.9},
                    'out':{'success':False, 'unexpected_list':['bdd'], 'unexpected_index_list':[3]}},
                {
                    'in':{'column':'b', 'regex':'^a', 'mostly':.75},
                    'out':{'success':True, 'unexpected_list':['bdd'], 'unexpected_index_list':[3]}},
                {
                    'in':{'column':'b', 'regex':'^a', 'mostly':.5},
                    'out':{'success':True, 'unexpected_list':['bdd'], 'unexpected_index_list':[3]}},
                {
                    'in':{'column':'c', 'regex':'^a'},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}},
                {
                    'in':{'column':'c', 'regex':'^a', 'mostly':.5},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list':[]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_match_regex(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])


    def test_expect_column_values_to_not_match_regex(self):
        #!!! Need to test mostly and suppress_exceptions

        D = ge.dataset.PandasDataSet({
            'x' : ['aa', 'ab', 'ac', 'a1', None, None, None],
            'y' : ['axxx', 'exxxx', 'ixxxx', 'oxxxxx', 'uxxxxx', 'yxxxxx', 'zxxxx'],
            'z' : [None, None, None, None, None, None, None]
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x', 'regex':'^a'},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2,3], 'unexpected_list':['aa', 'ab', 'ac', 'a1']}},
                {
                    'in':{'column':'x', 'regex':'^b'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'y', 'regex':'^z'},
                    'out':{'success':False, 'unexpected_index_list':[6], 'unexpected_list':['zxxxx']}}
        ]

        for t in T:
            out = D.expect_column_values_to_not_match_regex(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    # def test_expect_column_values_to_match_regex_list(self):
    #     with open("./tests/test_sets/expect_column_values_to_match_regex_list_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         out = D.expect_column_values_to_match_regex_list(**t['in'])
    #         self.assertEqual(t['out']['success'], out['success'])
    #         if 'unexpected_index_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #         if 'unexpected_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_values_to_match_strftime_format(self):
        """
        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'us_dates' : ['4/30/2017','4/30/2017','7/4/1776'],
            'us_dates_type_error' : ['4/30/2017','4/30/2017', 5],
            'almost_iso8601' : ['1977-05-25T00:00:00', '1980-05-21T13:47:59', '2017-06-12T23:57:59'],
            'almost_iso8601_val_error' : ['1977-05-55T00:00:00', '1980-05-21T13:47:59', '2017-06-12T23:57:59'],
            'already_datetime' : [datetime.datetime(2015,1,1), datetime.datetime(2016,1,1), datetime.datetime(2017,1,1)]
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'us_dates', 'strftime_format':'%m/%d/%Y'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}
                },
                {
                    'in':{'column':'us_dates_type_error','strftime_format':'%m/%d/%Y', 'mostly': 0.5, 'catch_exceptions': True},
                    # 'out':{'success':True, 'unexpected_index_list':[2], 'unexpected_list':[5]}},
                    'error':{
                        'traceback_substring' : 'TypeError'
                    },
                },
                {
                    'in':{'column':'us_dates_type_error','strftime_format':'%m/%d/%Y', 'catch_exceptions': True},
                    'error':{
                        'traceback_substring' : 'TypeError'
                    }
                },
                {
                    'in':{'column':'almost_iso8601','strftime_format':'%Y-%m-%dT%H:%M:%S'},
                    'out':{'success':True,'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'almost_iso8601_val_error','strftime_format':'%Y-%m-%dT%H:%M:%S'},
                    'out':{'success':False,'unexpected_index_list':[0], 'unexpected_list':['1977-05-55T00:00:00']}},
                {
                    'in':{'column':'already_datetime','strftime_format':'%Y-%m-%d', 'catch_exceptions':True},
                    # 'out':{'success':False,'unexpected_index_list':[0], 'unexpected_list':['1977-05-55T00:00:00']},
                    'error':{
                        'traceback_substring' : 'TypeError: Values passed to expect_column_values_to_match_strftime_format must be of type string.'
                    },
                }
        ]

        for t in T:
            out = D.expect_column_values_to_match_strftime_format(**t['in'])
            if 'out' in t:
                self.assertEqual(t['out']['success'], out['success'])
                if 'unexpected_index_list' in t['out']:
                    self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
                if 'unexpected_list' in t['out']:
                    self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])
            elif 'error' in t:
                self.assertEqual(out['exception_info']['raised_exception'], True)
                self.assertIn(t['error']['traceback_substring'], out['exception_info']['exception_traceback'])

    def test_expect_column_values_to_be_dateutil_parseable(self):

        D = ge.dataset.PandasDataSet({
            'c1':['03/06/09','23 April 1973','January 9, 2016'],
            'c2':['9/8/2012','covfefe',25],
            'c3':['Jared','June 1, 2013','July 18, 1976'],
            'c4':['1', '2', '49000004632'],
            'already_datetime' : [datetime.datetime(2015,1,1), datetime.datetime(2016,1,1), datetime.datetime(2017,1,1)],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column': 'c1'},
                    'out':{'success':True, 'unexpected_list':[], 'unexpected_index_list': []}},
                {
                    'in':{"column":'c2', "catch_exceptions":True},
                    # 'out':{'success':False, 'unexpected_list':['covfefe', 25], 'unexpected_index_list': [1, 2]}},
                    'error':{ 'traceback_substring' : 'TypeError: Values passed to expect_column_values_to_be_dateutil_parseable must be of type string' },
                },
                {
                    'in':{"column":'c3'},
                    'out':{'success':False, 'unexpected_list':['Jared'], 'unexpected_index_list': [0]}},
                {
                    'in':{'column': 'c3', 'mostly':.5},
                    'out':{'success':True, 'unexpected_list':['Jared'], 'unexpected_index_list': [0]}
                },
                {
                    'in':{'column': 'c4'},
                    'out':{'success':False, 'unexpected_list':['49000004632'], 'unexpected_index_list': [2]}
                },
                {
                    'in':{'column':'already_datetime', 'catch_exceptions':True},
                    'error':{ 'traceback_substring' : 'TypeError: Values passed to expect_column_values_to_be_dateutil_parseable must be of type string' },
                }
        ]

        for t in T:
            out = D.expect_column_values_to_be_dateutil_parseable(**t['in'])
            if 'out' in t:
                self.assertEqual(t['out']['success'], out['success'])
                self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
                self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])
            elif 'error' in t:
                self.assertEqual(out['exception_info']['raised_exception'], True)
                self.assertIn(t['error']['traceback_substring'], out['exception_info']['exception_traceback'])


    def test_expect_column_values_to_be_json_parseable(self):
        d1 = json.dumps({'i':[1,2,3],'j':35,'k':{'x':'five','y':5,'z':'101'}})
        d2 = json.dumps({'i':1,'j':2,'k':[3,4,5]})
        d3 = json.dumps({'i':'a', 'j':'b', 'k':'c'})
        d4 = json.dumps({'i':[4,5], 'j':[6,7], 'k':[8,9], 'l':{4:'x', 5:'y', 6:'z'}})
        D = ge.dataset.PandasDataSet({
            'json_col':[d1,d2,d3,d4],
            'not_json':[4,5,6,7],
            'py_dict':[{'a':1, 'out':1},{'b':2, 'out':4},{'c':3, 'out':9},{'d':4, 'out':16}],
            'most':[d1,d2,d3,'d4']
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'json_col'},
                    'out':{'success':True, 'unexpected_index_list':[], 'unexpected_list':[]}},
                {
                    'in':{'column':'not_json'},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2,3], 'unexpected_list':[4,5,6,7]}},
                {
                    'in':{'column':'py_dict'},
                    'out':{'success':False, 'unexpected_index_list':[0,1,2,3], 'unexpected_list':[{'a':1, 'out':1},{'b':2, 'out':4},{'c':3, 'out':9},{'d':4, 'out':16}]}},
                {
                    'in':{'column':'most'},
                    'out':{'success':False, 'unexpected_index_list':[3], 'unexpected_list':['d4']}},
                {
                    'in':{'column':'most', 'mostly':.75},
                    'out':{'success':True, 'unexpected_index_list':[3], 'unexpected_list':['d4']}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_json_parseable(**t['in'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    # def test_expect_column_values_to_match_json_schema(self):

    #     with open("./tests/test_sets/expect_column_values_to_match_json_schema_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         out = D.expect_column_values_to_match_json_schema(**t['in'])#, **t['kwargs'])
    #         self.assertEqual(t['out']['success'], out['success'])
    #         if 'unexpected_index_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #         if 'unexpected_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_median_to_be_between(self):
        ds = ge.dataset.PandasDataSet({
            'a': [0,1,2,3],
            'b': [0,1,1,2]
        })

        self.assertEqual(
            True,
            ds.expect_column_median_to_be_between('a', 1, 2)['success']
        )

        self.assertEqual(
            1.5,
            ds.expect_column_median_to_be_between('a', 1, 2)['result_obj']['observed_value']
        )

        self.assertEqual(
            1,
            ds.expect_column_median_to_be_between('b', 1, 1)['result_obj']['observed_value']
        )

    def test_expect_column_stdev_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,1,3],
            'dist2' : [-1,0,1]
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'dist1', 'min_value':.5, 'max_value':1.5},
                    'out':{'success':True, "result_obj": { "observed_value": D['dist1'].std(), "element_count": 3, "missing_count": 0, "missing_percent": 0}}},
                {
                    'in':{'column':'dist1', 'min_value':2, 'max_value':3},
                    'out':{'success':False, "result_obj": { "observed_value": D['dist1'].std(), "element_count": 3, "missing_count": 0, "missing_percent": 0}}},
                {
                    'in':{'column':'dist2', 'min_value':2, 'max_value':3},
                    'out':{'success':False, "result_obj": { "observed_value": 1, "element_count": 3, "missing_count": 0, "missing_percent": 0}}},
                {
                    'in':{'column':'dist2', 'min_value':0, 'max_value':1},
                    'out':{'success':True, "result_obj": { "observed_value": 1, "element_count": 3, "missing_count": 0, "missing_percent": 0}}}
        ]

        for t in T:
            out = D.expect_column_stdev_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])

        with self.assertRaises(ValueError):
            D.expect_column_stdev_to_be_between("dist1")


    def test_expect_column_unique_value_count_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,2,3,4,5,6,7,8],
            'dist2' : [1,2,3,4,5,None,None,None],
            'dist3' : [2,2,2,2,5,6,7,8],
            'dist4' : [1,1,1,1,None,None,None,None]
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{
                        'column': 'dist1',
                        'min_value': 0,
                        'max_value': 10
                    },
                    'kwargs':{},
                    'out':{'success':True, "result_obj": { "observed_value": 8, "element_count": 8, "missing_count": 0, "missing_percent": 0}}
                },{
                    'in':{
                        "column" : 'dist2',
                        "min_value" : None,
                        "max_value" : None
                    },
                    'kwargs':{},
                    'out':{'success':True, "result_obj": { "observed_value": 5, "element_count": 8, "missing_count": 3, "missing_percent": 3/8}}
                },{
                    'in':{
                        "column": 'dist3',
                        "min_value": None,
                        "max_value": 5
                    },
                    'kwargs':{},
                    'out':{'success':True, "result_obj": { "observed_value": 5, "element_count": 8, "missing_count": 0, "missing_percent": 0}}
                },{
                    'in':{
                        "column": 'dist4',
                        "min_value": 2,
                        "max_value": None
                    },
                    'kwargs':{},
                    'out':{'success':False, "result_obj": { "observed_value": 1, "element_count": 8, "missing_count": 4, "missing_percent": 0.5}}
                }
        ]

        for t in T:
            try:
                out = D.expect_column_unique_value_count_to_be_between(**t['in'])
                self.assertEqual(out, t['out'])
            except ValueError as err:
                self.assertEqual(str(err), "min_value and max_value cannot both be None")

    def test_expect_column_proportion_of_unique_values_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,1,3],
            'dist2' : [-1,0,1]
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'dist1', 'min_value':.5, 'max_value':1.5},
                    'out':{'success':True, "result_obj": { "observed_value": 2/3, "element_count": 3, "missing_count": 0, "missing_percent": 0}}},
                {
                    'in':{'column':'dist1', 'min_value':2, 'max_value':3},
                    'out':{'success':False, "result_obj": { "observed_value": 2/3, "element_count": 3, "missing_count": 0, "missing_percent": 0}}},
                {
                    'in':{'column':'dist2', 'min_value':2, 'max_value':3},
                    'out':{'success':False, "result_obj": { "observed_value": 1, "element_count": 3, "missing_count": 0, "missing_percent": 0}}},
                {
                    'in':{'column':'dist2', 'min_value':0, 'max_value':1},
                    'out':{'success':True, "result_obj": { "observed_value": 1, "element_count": 3, "missing_count": 0, "missing_percent": 0}}}
        ]

        for t in T:
            out = D.expect_column_proportion_of_unique_values_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])

    # def test_expect_column_values_to_be_increasing(self):
    #     print("=== test_expect_column_values_to_be_increasing ===")
    #     with open("./tests/test_sets/expect_column_values_to_be_increasing_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         print(t)
    #         out = D.expect_column_values_to_be_increasing(**t['in'])#, **t['kwargs'])
    #         self.assertEqual(t['out']['success'], out['success'])
    #         if 'unexpected_index_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #         if 'unexpected_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    # def test_expect_column_values_to_be_decreasing(self):
    #     print("=== test_expect_column_values_to_be_decreasing ===")
    #     with open("./tests/test_sets/expect_column_values_to_be_decreasing_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         print(t)
    #         out = D.expect_column_values_to_be_decreasing(**t['in'])
    #         self.assertEqual(t['out']['success'], out['success'])
    #         if 'unexpected_index_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #         if 'unexpected_list' in t['out']:
    #             self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_expect_column_most_common_value_to_be_in_set(self):

        D = ge.dataset.PandasDataSet({
            'x' : [1,1,2,2,3,None, None, None, None, None],
            'y' : ['hello', 'jello', 'mello', 'hello', 'jello', 'mello', 'hello', 'jello', 'mello', 'jello'],
            'z' : [1,2,2,3,3,3,4,4,4,4],
        })
        D.set_default_expectation_argument("result_format", "COMPLETE")

        T = [
                {
                    'in':{"column":"x","value_set":[1]},
                    'out':{"success":False, "result_obj": { "observed_value": [1,2], "element_count": 10, "missing_count": 5, "missing_percent": 0.5}},
                },{
                    'in':{"column":"x", "value_set":[1], "ties_okay":True},
                    'out':{"success":True, "result_obj": { "observed_value": [1,2], "element_count": 10, "missing_count": 5, "missing_percent": 0.5}},
                },{
                    'in':{"column":"x","value_set":[3]},
                    'out':{"success":False, "result_obj": { "observed_value": [1,2], "element_count": 10, "missing_count": 5, "missing_percent": 0.5}},
                },{
                    'in':{"column":"y","value_set":["jello", "hello"]},
                    'out':{'success':True, "result_obj": { "observed_value": ["jello"], "element_count": 10, "missing_count": 0, "missing_percent": 0}},
                },{
                    'in':{"column":"y","value_set":["hello", "mello"]},
                    'out':{'success':False, "result_obj": { "observed_value": ["jello"], "element_count": 10, "missing_count": 0, "missing_percent": 0}},
                },{
                    'in':{"column":"z","value_set":[4]},
                    'out':{'success':True, "result_obj": { "observed_value": [4], "element_count": 10, "missing_count": 0, "missing_percent": 0}},
                }
        ]


        for t in T:
            out = D.expect_column_most_common_value_to_be_in_set(**t['in'])
            self.assertEqual(out, t['out'])


    # def test_expect_column_sum_to_be_between(self):
    #     with open("./tests/test_sets/expect_column_sum_to_be_between_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         print(json.dumps(t))
    #         out = D.expect_column_sum_to_be_between(**t['in'])
    #         print(out)

    #         if "out" in t:
    #             self.assertEqual(t['out']['success'], out['success'])
    #             if 'unexpected_index_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #             if 'unexpected_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    #         if "error" in t:
    #             self.assertEqual(out['exception_info']['raised_exception'], True)
    #             self.assertIn(t['error']['traceback_substring'], out['exception_info']['exception_traceback'])

    # def test_expect_column_min_to_be_between(self):
    #     with open("./tests/test_sets/expect_column_min_to_be_between_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         print(json.dumps(t))
    #         out = D.expect_column_min_to_be_between(**t['in'])
    #         print(out)

    #         if "out" in t:
    #             self.assertEqual(t['out']['success'], out['success'])
    #             if 'unexpected_index_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #             if 'unexpected_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    #         if "error" in t:
    #             self.assertEqual(out['exception_info']['raised_exception'], True)
    #             self.assertIn(t['error']['traceback_substring'], out['exception_info']['exception_traceback'])

    # def test_expect_column_max_to_be_between(self):
    #     with open("./tests/test_sets/expect_column_max_to_be_between_test_set.json") as f:
    #         J = json.load(f)
    #         D = ge.dataset.PandasDataSet(J["dataset"])
    #         D.set_default_expectation_argument("result_format", "COMPLETE")
    #         T = J["tests"]

    #         self.maxDiff = None

    #     for t in T:
    #         print(json.dumps(t))
    #         out = D.expect_column_max_to_be_between(**t['in'])
    #         print(out)

    #         if "out" in t:
    #             self.assertEqual(t['out']['success'], out['success'])
    #             if 'unexpected_index_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
    #             if 'unexpected_list' in t['out']:
    #                 self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    #         if "error" in t:
    #             self.assertEqual(out['raised_exception'], True)
    #             self.assertIn(t['error']['traceback_substring'], out['exception_traceback'])

    def test_expectation_decorator_summary_mode(self):

        df = ge.dataset.PandasDataSet({
            'x' : [1,2,3,4,5,6,7,7,None,None],
        })
        df.set_default_expectation_argument("result_format", "COMPLETE")

        # print '&'*80
        # print json.dumps(df.expect_column_values_to_be_between('x', min_value=1, max_value=5, result_format="SUMMARY"), indent=2)

        self.maxDiff = None
        self.assertEqual(
            df.expect_column_values_to_be_between('x', min_value=1, max_value=5, result_format="SUMMARY"),
            {
                "success" : False,
                "result_obj" : {
                    "element_count" : 10,
                    "missing_count" : 2,
                    "missing_percent" : .2,
                    "unexpected_count" : 3,
                    "partial_unexpected_counts": [
                        {"value": 7.0,
                         "count": 2},
                        {"value": 6.0,
                         "count": 1}
                    ],
                    "unexpected_percent": 0.3,
                    "unexpected_percent_nonmissing": 0.375,
                    "partial_unexpected_list" : [6.0,7.0,7.0],
                    "partial_unexpected_index_list": [5,6,7],
                }
            }
        )

        self.assertEqual(
            df.expect_column_mean_to_be_between("x", 3, 7, result_format="SUMMARY"),
            {
                'success': True,
                'result_obj': {
                    'observed_value': 4.375,
                    'element_count': 10,
                    'missing_count': 2,
                    'missing_percent': .2
                },
            }
        )

    def test_positional_arguments(self):

        df = ge.dataset.PandasDataSet({
            'x':[1,3,5,7,9],
            'y':[2,4,6,8,10],
            'z':[None,'a','b','c','abc']
        })
        df.set_default_expectation_argument('result_format', 'COMPLETE')

        self.assertEqual(
            df.expect_column_mean_to_be_between('x',4,6),
            {'success':True, 'result_obj': {'observed_value': 5, 'element_count': 5,
                'missing_count': 0,
                'missing_percent': 0.0}}
        )

        out = df.expect_column_values_to_be_between('y',1,6)
        t = {'out': {'success':False, 'unexpected_list':[8,10], 'unexpected_index_list': [3,4]}}
        if 'out' in t:
            self.assertEqual(t['out']['success'], out['success'])
            if 'unexpected_index_list' in t['out']:
                self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            if 'unexpected_list' in t['out']:
                self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        out = df.expect_column_values_to_be_between('y',1,6,mostly=.5)
        t = {'out': {'success':True, 'unexpected_list':[8,10], 'unexpected_index_list':[3,4]}}
        if 'out' in t:
            self.assertEqual(t['out']['success'], out['success'])
            if 'unexpected_index_list' in t['out']:
                self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            if 'unexpected_list' in t['out']:
                self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        out = df.expect_column_values_to_be_in_set('z',['a','b','c'])
        t = {'out': {'success':False, 'unexpected_list':['abc'], 'unexpected_index_list':[4]}}
        if 'out' in t:
            self.assertEqual(t['out']['success'], out['success'])
            if 'unexpected_index_list' in t['out']:
                self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            if 'unexpected_list' in t['out']:
                self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

        out = df.expect_column_values_to_be_in_set('z',['a','b','c'],mostly=.5)
        t = {'out': {'success':True, 'unexpected_list':['abc'], 'unexpected_index_list':[4]}}
        if 'out' in t:
            self.assertEqual(t['out']['success'], out['success'])
            if 'unexpected_index_list' in t['out']:
                self.assertEqual(t['out']['unexpected_index_list'], out['result_obj']['unexpected_index_list'])
            if 'unexpected_list' in t['out']:
                self.assertEqual(t['out']['unexpected_list'], out['result_obj']['unexpected_list'])

    def test_result_format_argument_in_decorators(self):
        df = ge.dataset.PandasDataSet({
            'x':[1,3,5,7,9],
            'y':[2,4,6,8,10],
            'z':[None,'a','b','c','abc']
        })
        df.set_default_expectation_argument('result_format', 'COMPLETE')

        #Test explicit Nones in result_format
        self.assertEqual(
            df.expect_column_mean_to_be_between('x',4,6, result_format=None),
            {'success':True, 'result_obj': {'observed_value': 5, 'element_count': 5,
                'missing_count': 0,
                'missing_percent': 0.0
                }}
        )

        self.assertEqual(
            df.expect_column_values_to_be_between('y',1,6, result_format=None),
            {'result_obj': {'element_count': 5,
                            'missing_count': 0,
                            'missing_percent': 0.0,
                            'partial_unexpected_counts': [{'count': 1, 'value': 8},
                                                          {'count': 1, 'value': 10}],
                            'partial_unexpected_index_list': [3, 4],
                            'partial_unexpected_list': [8, 10],
                            'unexpected_count': 2,
                            'unexpected_index_list': [3, 4],
                            'unexpected_list': [8, 10],
                            'unexpected_percent': 0.4,
                            'unexpected_percent_nonmissing': 0.4},
             'success': False}
        )

        #Test unknown output format
        with self.assertRaises(ValueError):
            df.expect_column_values_to_be_between('y',1,6, result_format="QUACK")

        with self.assertRaises(ValueError):
            df.expect_column_mean_to_be_between('x',4,6, result_format="QUACK")

if __name__ == "__main__":
    unittest.main()
