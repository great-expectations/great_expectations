import unittest
import json
import hashlib
import datetime
import numpy as np

import great_expectations as ge


class TestPandasDataset(unittest.TestCase):

    def test_expect_table_row_count_to_be_between(self):

        # Data for testing
        D = ge.dataset.PandasDataSet({
            'c1' : [4,5,6,7],
            'c2' : ['a','b','c','d'],
            'c3' : [None,None,None,None]
        })

        # Tests
        T = [
                {
                    'in':[3,5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':4}},
                {
                    'in':[0,1],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':4}},
                {
                    'in':[4,4],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':4}},
                {
                    'in':[1,0],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':4}}
        ]

        for t in T:
            out = D.expect_table_row_count_to_be_between(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])

        D = ge.dataset.PandasDataSet({
            'c1':[1,None,3,None,5],
            'c2':[None,4,5,None,None],
            'c3':[None,None,None,None,None]
        })

        T = [
                {
                    'in':[5,6],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':5}},
                {
                    'in':[2,4],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':5}},
                {
                    'in':[5,5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':5}},
                {
                    'in':[2,1],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':5}}
        ]

        for t in T:
            out = D.expect_table_row_count_to_be_between(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_table_row_count_to_equal(self):

        D = ge.dataset.PandasDataSet({
            'c1':[4,5,6,7],
            'c2':['a','b','c','d'],
            'c3':[None,None,None,None]
        })

        # Tests
        T = [
                {
                    'in':[4],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':4}},
                {
                    'in':[5],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':4}},
                {
                    'in':[3],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':4}},
                {
                    'in':[0],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':4}}
        ]

        for t in T:
            out = D.expect_table_row_count_to_equal(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])

        D = ge.dataset.PandasDataSet({
            'c1':[1,None,3,None,5],
            'c2':[None,4,5,None,None],
            'c3':[None,None,None,None,None]
        })

        T = [
                {
                    'in':[5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':5}},
                {
                    'in':[3],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':5}}
        ]

        for t in T:
            out = D.expect_table_row_count_to_equal(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_values_to_be_unique(self):

        D = ge.dataset.PandasDataSet({
            'a' : ['2', '2'],
            'b' : [1, '2'],
            'c' : [1, 1],
            'd' : [1, '1'],
            'n' : [None, np.nan]
        })

        # Tests for D
        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['2']}},
                {
                    'in':['b'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['c'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1]}},
                {
                    'in':['d'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['n'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['n'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['a'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_unique(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


        df = ge.dataset.PandasDataSet({
            'a' : ['2', '2', '2', '2'],
            'b' : [1, '2', '2', '3'],
            'n' : [None, None, np.nan, None],
        })

        # Tests for df
        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['2','2','2']}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.25},
                    'out':{'success':True, 'exception_list':['2']}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.75},
                    'out':{'success':False, 'exception_list':['2']}},
                {
                    'in':['a'],
                    'kwargs':{'mostly':1},
                    'out':{'success':False, 'exception_list':['2','2','2']}},
                {
                    'in':['n'],
                    'kwargs':{'mostly':.2},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['a'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}}
        ]

        for t in T:
            out = df.expect_column_values_to_be_unique(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])



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

        T = [
                {
                    'in':['y'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[None]}},
                {
                    'in':['n'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[None, None]}},
                {
                    'in':['z'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['x'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['n'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['z'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}}
        ]


        for t in T:
            out = D.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


        D2 = ge.dataset.PandasDataSet({
            'a' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'b' : [1, 2, 3, 4, 5, 6, 7, 8, 9, None],
        })

        #assert_equal(
        #    D.expect_column_values_to_not_be_null('x'),
        #    {'success':False, 'exception_list':[None]}
        #)

        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['a'],
                    'kwargs':{'mostly':.90},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['b'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[None]}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.95},
                    'out':{'success':False, 'exception_list':[None]}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.90},
                    'out':{'success':True, 'exception_list':[None]}},
                {
                    'in':['b'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.95, 'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.90, 'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}}
        ]

        for t in T:
            out = D2.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


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

        T = [
                {
                    'in':['x'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[2,2]}},
                {
                    'in':['y'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[2,2]}},
                {
                    'in':['z'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[2,5,7]}},
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['x'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['x'],
                    'kwargs':{'mostly':.2, 'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}},
                {
                    'in':['x'],
                    'kwargs':{'mostly':.8, 'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}
                    },
                {
                    'in':['a'],
                    'kwargs':{'mostly':.5, 'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_null(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


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

        T = [
                {
                    'in':['x','int','python'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['x','string','numpy'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1,2,4]}},
                {
                    'in':['y','float','python'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['y','float','numpy'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1.0,2.2,5.3]}},
                {
                    'in':['z','string','python'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['b','boolean','python'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}}
                #{
                #    'in':['n','null','python'],
                #    'kwargs':{},
                #    'out':{'success':False, 'exception_list':[np.nan]}},
                #{
                #    'in':['n','null','python'],
                #    'kwargs':{'mostly':.5},
                #    'out':{'success':True, 'exception_list':[np.nan]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_of_type(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_values_to_be_in_set(self):
        """
        Cases Tested:

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })

        T = [
                {
                    'in':['x', [1,2,4]],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['x', [4,2]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1]}},
                {
                    'in':['y', []],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1,2,5]}},
                {
                    'in':['z', ['hello','jello','mello']],
                    'kwargs':{},
                    'out': {'success':True, 'exception_list':[]}},
                {
                    'in':['z', ['hello']],
                    'kwargs':{},
                    'out': {'success':False, 'exception_list':['jello','mello']}},
                {
                    'in':['x', [1,2,4]],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['x', [4,2]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1]}},
                {
                    'in':['y', []],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1,2,5]}},
                {
                    'in':['z', ['hello', 'jello', 'mello']],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['z', ['hello']],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['jello', 'mello']}}

        ]

        for t in T:
            out = D.expect_column_values_to_be_in_set(*t['in'], **t['kwargs'])
            self.assertEqual(out,t['out'])


        D2 = ge.dataset.PandasDataSet({
            'x' : [1,1,2,None],
            'y' : [None,None,None,None],
        })

        T = [
                {
                    'in':['x',[1,2]],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['x',[1]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[2]}},
                {
                    'in':['x',[2]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1, 1]}},
                {
                    'in':['x',[1]],
                    'kwargs':{'mostly':.66},
                    'out':{'success':True, 'exception_list':[2]}},
                {
                    'in':['x',[1]],
                    'kwargs':{'mostly':.33},
                    'out':{'success':True, 'exception_list':[2]}},
                {
                    'in':['x',[2]],
                    'kwargs':{'mostly':.66},
                    'out':{'success':False, 'exception_list':[1,1]}},
                {
                    'in':['x',[2]],
                    'kwargs':{'mostly':.9},
                    'out':{'success':False, 'exception_list':[1,1]}},
                {
                    'in':['y',[2]],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['y',[]],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['y',[2]],
                    'kwargs':{'mostly':.5},
                    'out':{'success':True, 'exception_list':[]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_be_in_set(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_values_to_not_be_in_set(self):
        """
        Cases Tested:
        -Repeat values being returned
        -Running expectations only on nonmissing values
        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
            'a' : [1,1,2],
            'n' : [None,None,2],
        })

        T = [
                {
                    'in':['x',[1,2]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1,2]}},
                {
                    'in':['x',[5,6]],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['z',['hello', 'jello']],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['hello', 'jello']}},
                {
                    'in':['z',[]],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['a', [1]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[1, 1]}},
                {
                    'in':['n', [2]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[2]}},
                {
                    'in':['n', []],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['n', [2]],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['a', [1]],
                    'kwargs':{'mostly':.2, 'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}},
                {
                    'in':['n', [2]],
                    'kwargs':{'mostly':1},
                    'out':{'success':False, 'exception_list':[2]}}
        ]

        for t in T:
            out = D.expect_column_values_to_not_be_in_set(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])



    def test_expect_column_values_to_be_between(self):
        """

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'y' : [1, 2, 3, 4, 5, 6, 7, 8, 9, "abc"],
            'z' : [1, 2, 3, 4, 5, None, None, None, None, None],
        })

        T = json.load(file("./tests/test_sets/expect_column_values_to_be_between_test_set_ADJ.json"))
        # print json.dumps(T, indent=2)

        for t in T:
            out = D.expect_column_values_to_be_between(**t['in'])#, **t['kwargs'])
            self.assertEqual(out, t['out'])

    def test_expect_column_value_lengths_to_be_between(self):
        D = ge.dataset.PandasDataSet({
            's1':['smart','silly','sassy','slimy','sexy'],
            's2':['cool','calm','collected','casual','creepy']
        })

        T = [
                {
                    'in':['s1', 3, 5],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['s2', 4, 6],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['collected']}},
                {
                    'in':['s2', None, 10],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}}
        ]

        #out1 = D.expect_column_value_lengths_to_be_between('s1', min_value=3, max_value=5)
        #out2 = D.expect_column_value_lengths_to_be_between('s2', min_value=4, max_value=6)
        #out3 = D.expect_column_value_lengths_to_be_between('s2', min_value=None, max_value=10)
        #assert_equal( out1['success'], True)
        #assert_equal( out2['success'], False)
        #assert_equal( len(out2['exception_list']), 1)
        #assert_equal( out3['success'], True)

        for t in T:
            out = D.expect_column_value_lengths_to_be_between(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])



    def test_expect_column_values_to_match_regex(self):
        """
        Cases Tested:
            Tested mostly alphabet regex
        """

        D = ge.dataset.PandasDataSet({
            'x' : ['aa', 'ab', 'ac', 'a1', None],
            'y' : ['aa', 'ab', 'ac', 'ba', 'ca'],
        })


        D2 = ge.dataset.PandasDataSet({
            'a' : ['aaa', 'abb', 'acc', 'add', 'bee'],
            'b' : ['aaa', 'abb', 'acc', 'bdd', None],
            'c' : [ None,  None,  None,  None, None],
        })
        T = [
                {
                    'in':['x', '^a'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['x', 'aa'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['ab', 'ac', 'a1']}},
                {
                    'in':['x', 'a[a-z]'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['a1']}},
                {
                    'in':['y', '[abc]{2}'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['y', '[z]'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['aa', 'ab', 'ac', 'ba', 'ca']}},
                {
                    'in':['y', '[abc]{2}'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}},
                {
                    'in':['y', '[z]'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}}
        ]

        for t in T:
            out = D.expect_column_values_to_match_regex(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])

            T = [
                    {
                        'in':['a', '^a'],
                        'kwargs':{'mostly':.9},
                        'out':{'success':False, 'exception_list':['bee']}},
                    {
                        'in':['a', '^a'],
                        'kwargs':{'mostly':.8},
                        'out':{'success':True, 'exception_list':['bee']}},
                    {
                        'in':['a', '^a'],
                        'kwargs':{'mostly':.7},
                        'out':{'success':True, 'exception_list':['bee']}},
                    {
                        'in':['b', '^a'],
                        'kwargs':{'mostly':.9},
                        'out':{'success':False, 'exception_list':['bdd']}},
                    {
                        'in':['b', '^a'],
                        'kwargs':{'mostly':.75},
                        'out':{'success':True, 'exception_list':['bdd']}},
                    {
                        'in':['b', '^a'],
                        'kwargs':{'mostly':.5},
                        'out':{'success':True, 'exception_list':['bdd']}},
                    {
                        'in':['b', '^a'],
                        'kwargs':{'mostly':.9, 'suppress_exceptions':True},
                        'out':{'success':False, 'exception_list':None}},
                    {
                        'in':['b', '^a'],
                        'kwargs':{'mostly':.75, 'suppress_exceptions':True},
                        'out':{'success':True, 'exception_list':None}},
                    {
                        'in':['b', '^a'],
                        'kwargs':{'mostly':.5, 'suppress_exceptions':True},
                        'out':{'success':True, 'exception_list':None}},
                    {
                        'in':['c', '^a'],
                        'kwargs':{},
                        'out':{'success':True, 'exception_list':[]}},
                    {
                        'in':['c', '^a'],
                        'kwargs':{'mostly':.5},
                        'out':{'success':True, 'exception_list':[]}},
                    {
                        'in':['c', '^a'],
                        'kwargs':{'suppress_exceptions':True},
                        'out':{'success':True, 'exception_list':[]}},
                    {
                        'in':['c', '^a'],
                        'kwargs':{'mostly':.5, 'suppress_exceptions':True},
                        'out':{'success':True, 'exception_list':[]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_match_regex(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])



    def test_expect_column_values_to_not_match_regex(self):
        #!!! Need to test mostly and suppress_exceptions

        D = ge.dataset.PandasDataSet({
            'x' : ['aa', 'ab', 'ac', 'a1', None, None, None],
            'y' : ['axxx', 'exxxx', 'ixxxx', 'oxxxxx', 'uxxxxx', 'yxxxxx', 'zxxxx'],
            'z' : [None, None, None, None, None, None, None]
        })

        T = [
                {
                    'in':['x', '^a'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['aa', 'ab', 'ac', 'a1']}},
                {
                    'in':['x', '^b'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['y', '^z'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['zxxxx']}},
                {
                    'in':['y', '^z'],
                    'kwargs':{'mostly':.5},
                    'out':{'success':True, 'exception_list':['zxxxx']}},
                {
                    'in':['x', '^a'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['x', '^b'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}},
                {
                    'in':['y', '^z'],
                    'kwargs':{'suppress_exceptions':True},
                    'out':{'success':False, 'exception_list':None}},
                {
                    'in':['y', '^z'],
                    'kwargs':{'mostly':.5, 'suppress_exceptions':True},
                    'out':{'success':True, 'exception_list':None}}
        ]

        # Why are these here?
        #assert D.expect_column_values_to_match_regex('z', '^a') == {'success':True, 'exception_list':[]}
        #assert D.expect_column_values_to_match_regex('z', '^a', mostly=.5) == {'success':True, 'exception_list':[]}
        #assert D.expect_column_values_to_match_regex('z', '^a', suppress_exceptions=True) == {'success':True, 'exception_list':[]}
        #assert D.expect_column_values_to_match_regex('z', '^a', mostly=.5, suppress_exceptions=True) == {'success':True, 'exception_list':[]}

        for t in T:
            out = D.expect_column_values_to_not_match_regex(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_values_to_match_regex_list(self):
        self.assertRaises(NotImplementedError)


    def test_expect_column_values_to_match_strftime_format(self):
        """
        Cases Tested:


        !!! TODO: Add tests for in types and raised exceptions

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'us_dates' : ['4/30/2017','4/30/2017','7/4/1776'],
            'us_dates_type_error' : ['4/30/2017','4/30/2017', 5],
            'almost_iso8601' : ['1977-05-25T00:00:00', '1980-05-21T13:47:59', '2017-06-12T23:57:59'],
            'almost_iso8601_val_error' : ['1977-05-55T00:00:00', '1980-05-21T13:47:59', '2017-06-12T23:57:59']
        })

        T = [
                {
                    'in':['us_dates','%m/%d/%Y'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['us_dates_type_error','%m/%d/%Y'],
                    'kwargs':{'mostly': 0.5},
                    'out':{'success':True, 'exception_list':[5]}},
                {
                    'in':['us_dates_type_error','%m/%d/%Y'],
                    'kwargs':{},
                    'out':{'success':False,'exception_list':[5]}},
                {
                    'in':['almost_iso8601','%Y-%m-%dT%H:%M:%S'],
                    'kwargs':{},
                    'out':{'success':True,'exception_list':[]}},
                {
                    'in':['almost_iso8601_val_error','%Y-%m-%dT%H:%M:%S'],
                    'kwargs':{},
                    'out':{'success':False,'exception_list':['1977-05-55T00:00:00']}}
        ]

        for t in T:
            out = D.expect_column_values_to_match_strftime_format(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_values_to_be_dateutil_parseable(self):

        D = ge.dataset.PandasDataSet({
            'c1':['03/06/09','23 April 1973','January 9, 2016'],
            'c2':['9/8/2012','covfefe',25],
            'c3':['Jared','June 1, 2013','July 18, 1976']
        })

        T = [
                {
                    'in':['c1'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['c2'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['covfefe', 25]}},
                {
                    'in':['c3'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['Jared']}},
                {
                    'in':['c3'],
                    'kwargs':{'mostly':.5},
                    'out':{'success':True, 'exception_list':['Jared']}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_dateutil_parseable(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_values_to_be_valid_json(self):
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

        T = [
                {
                    'in':['json_col'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[]}},
                {
                    'in':['not_json'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[4,5,6,7]}},
                {
                    'in':['py_dict'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':[{'a':1, 'out':1},{'b':2, 'out':4},{'c':3, 'out':9},{'d':4, 'out':16}]}},
                {
                    'in':['most'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['d4']}},
                {
                    'in':['most'],
                    'kwargs':{'mostly':.75},
                    'out':{'success':True, 'exception_list':['d4']}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_valid_json(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_mean_to_be_between(self):
        """
        #!!! Ignores null (None and np.nan) values. If all null values, return {'success':False, 'exception_list':None)
        Cases Tested:
            Tested with float - float
            Tested with float - int
            Tested with np.nap
        """

        D = ge.dataset.PandasDataSet({
            'x' : [2.0, 5.0],
            'y' : [5.0, 5],
            'z' : [0, 10],
            'n' : [0, None],
            's' : ['s', np.nan],
            'b' : [True, False],
        })

        T = [
                {
                    'in':['x', 2, 5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':3.5}},
                {
                    'in':['x', 1, 2],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':3.5}},
                {
                    'in':['y', 5, 5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':5}},
                {
                    'in':['y', 4, 4],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':5}},
                {
                    'in':['z', 5, 5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':5}},
                {
                    'in':['z', 13, 14],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':5}},
                {
                    'in':['n', 0, 0],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':0.0}}
        ]

        typedf = ge.dataset.PandasDataSet({
            's' : ['s', np.nan, None, None],
            'b' : [True, False, False, True],
            'x' : [True, None, False, None],
        })


        for t in T:
            out = D.expect_column_mean_to_be_between(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])

        T = [
                {
                    'in':['s', 0, 0],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':None}},
                {
                    'in':['b', 0, 1],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':0.5}},
                {
                    'in':['x', 0, 1],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':0.5}}
        ]

        for t in T:
            out = typedf.expect_column_mean_to_be_between(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])



    def test_expect_column_stdev_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,1,3],
            'dist2' : [-1,0,1]
        })

        T = [
                {
                    'in':['dist1',.5,1.5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':D['dist1'].std()}},
                {
                    'in':['dist1',2,3],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':D['dist1'].std()}},
                {
                    'in':['dist2',2,3],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':1.0}},
                {
                    'in':['dist2',0,1],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':1.0}}
        ]

        for t in T:
            out = D.expect_column_stdev_to_be_between(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


    def test_expect_column_unique_value_count_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,2,3,4,5,6,7,8],
            'dist2' : [1,2,3,4,5,None,None,None],
            'dist3' : [2,2,2,2,5,6,7,8],
            'dist4' : [1,1,1,1,None,None,None,None]
        })

        T = [
                {
                    'in':{
                        'column': 'dist1',
                        'min_value': 0,
                        'max_value': 10
                    },
                    'kwargs':{},
                    'out':{'success':True, 'true_value':8}
                },{
                    'in':{
                        "column" : 'dist2',
                        "min_value" : None,
                        "max_value" : None
                    },
                    'kwargs':{},
                    'out':{'success':True, 'true_value':5}
                },{
                    'in':{
                        "column": 'dist3',
                        "min_value": None,
                        "max_value": 5
                    },
                    'kwargs':{},
                    'out':{'success':True, 'true_value':5}
                },{
                    'in':{
                        "column": 'dist4',
                        "min_value": 2,
                        "max_value": None
                    },
                    'kwargs':{},
                    'out':{'success':False, 'true_value':1}
                }
        ]

        for t in T:
            # print t['in'], t['out']
            out = D.expect_column_unique_value_count_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])

    def test_expect_column_unique_proportion_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,1,3],
            'dist2' : [-1,0,1]
        })

        T = [
                {
                    'in':['dist1',.5,1.5],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':D['dist1'].std()}},
                {
                    'in':['dist1',2,3],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':D['dist1'].std()}},
                {
                    'in':['dist2',2,3],
                    'kwargs':{},
                    'out':{'success':False, 'true_value':1.0}},
                {
                    'in':['dist2',0,1],
                    'kwargs':{},
                    'out':{'success':True, 'true_value':1.0}}
        ]

        for t in T:
            out = D.expect_column_stdev_to_be_between(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])

    def test_expectation_decorator_summary_mode(self):

        df = ge.dataset.PandasDataSet({
            'x' : [1,2,3,4,5,6,7,7,None,None],
        })

        self.maxDiff = None
        self.assertEqual(
            df.expect_column_values_to_be_between('x', min_value=1, max_value=5, output_format="SUMMARY"),
            {
                "success" : False,
                "exception_list" : [6.0,7.0,7.0],
                "exception_index_list": [5,6,7],
                "element_count" : 10,
                "missing_count" : 2,
                "missing_percent" : .2,
                "exception_count" : 3,
                "exception_counts": {
                    6.0 : 1,
                    7.0 : 2,
                },
                "exception_percent": 0.3,
                "exception_percent_nonmissing": 0.375,
            }
        )



if __name__ == "__main__":
    unittest.main()
