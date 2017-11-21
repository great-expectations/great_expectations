import unittest
import json
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
        D.set_default_expectation_argument("output_format", "COMPLETE")

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
        D.set_default_expectation_argument("output_format", "COMPLETE")

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
        D.set_default_expectation_argument("output_format", "COMPLETE")

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
        D.set_default_expectation_argument("output_format", "COMPLETE")

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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        # Tests for D
        T = [
                {
                    'in':{'column':'a'},
                    'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':['2','2']}},
                {
                    'in':{'column':'b'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'c'},
                    'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':[1,1]}},
                {
                    'in':{'column':'d'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'n'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_unique(**t['in'])
            self.assertEqual(out, t['out'])


        df = ge.dataset.PandasDataSet({
            'a' : ['2', '2', '2', '2'],
            'b' : [1, '2', '2', '3'],
            'n' : [None, None, np.nan, None],
        })
        df.set_default_expectation_argument("output_format", "COMPLETE")

        # Tests for df
        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_index_list':[0,1,2,3], 'exception_list':['2','2','2','2']}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.25},
                    'out':{'success':True, 'exception_index_list':[1,2], 'exception_list':['2','2']}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.75},
                    'out':{'success':False, 'exception_index_list':[1,2], 'exception_list':['2','2']}},
                {
                    'in':['a'],
                    'kwargs':{'mostly':1},
                    'out':{'success':False, 'exception_index_list':[0,1,2,3], 'exception_list':['2','2','2','2']}},
                {
                    'in':['n'],
                    'kwargs':{'mostly':.2},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}}
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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'y'},
                    'out':{'success':False, 'exception_index_list':[1], 'exception_list':[None]}},
                {
                    'in':{'column':'n'},
                    'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':[None, None]}},
                # {
                #     'in':{'column':'y'},
                #     'out':{'success':False, 'exception_index_list':[1], 'exception_list':[np.nan]}},
                # {
                #     'in':{'column':'n'},
                #     'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':[None, np.nan]}},
                {
                    'in':{'column':'x'},
                    'out':{'success':False, 'exception_index_list':[1], 'exception_list':[None]}},
                {
                    'in':{'column':'z'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}}
        ]


        for t in T:
            out = D.expect_column_values_to_not_be_null(**t['in'])
            self.assertEqual(out, t['out'])


        D2 = ge.dataset.PandasDataSet({
            'a' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'b' : [1, 2, 3, 4, 5, 6, 7, 8, 9, None],
        })
        D2.set_default_expectation_argument("output_format", "COMPLETE")

        #assert_equal(
        #    D.expect_column_values_to_not_be_null('x'),
        #    {'success':False, 'exception_list':[None]}
        #)

        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':['a'],
                    'kwargs':{'mostly':.90},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':['b'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_index_list':[9], 'exception_list':[None]}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.95},
                    'out':{'success':False, 'exception_index_list':[9], 'exception_list':[None]}},
                {
                    'in':['b'],
                    'kwargs':{'mostly':.90},
                    'out':{'success':True, 'exception_index_list':[9], 'exception_list':[None]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])


        D3 = ge.dataset.PandasDataSet({
            'a' : [None, None, None, None],
        })
        D3.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_index_list':[0,1,2,3], 'exception_list':[None,None,None,None]}
                },
                {
                    'in':['a'],
                    'kwargs':{"mostly":.95},
                    'out':{'success':False, 'exception_index_list':[0,1,2,3], 'exception_list':[None,None,None,None]}
                },
        ]

        for t in T:
            out = D3.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            # out = D3.expect_column_values_to_be_null(*t['in'], **t['kwargs'])
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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x'},
                    'out':{'success':False, 'exception_index_list':[0,2], 'exception_list':[2,2]}},
                {
                    'in':{'column':'y'},
                    'out':{'success':False, 'exception_index_list':[0,2], 'exception_list':[2,2]}},
                {
                    'in':{'column':'z'},
                    'out':{'success':False, 'exception_index_list':[0,1,2], 'exception_list':[2,5,7]}},
                {
                    'in':{'column':'a'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'x', 'mostly':.2},
                    'out':{'success':True, 'exception_index_list':[0,2], 'exception_list':[2,2]}},
                {
                    'in':{'column':'x', 'mostly':.8},
                    'out':{'success':False, 'exception_index_list':[0,2], 'exception_list':[2,2]}
                    },
                {
                    'in':{'column':'a', 'mostly':.5},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_null(**t['in'])
            self.assertEqual(out, t['out'])


        D3 = ge.dataset.PandasDataSet({
            'a' : [None, None, None, None],
            'b' : [np.nan, np.nan, np.nan, np.nan],
        })
        D3.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':['a'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}
                },
                {
                    'in':['a'],
                    'kwargs':{"mostly":.95},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}
                },
                {
                    'in':['b'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}
                },
                {
                    'in':['b'],
                    'kwargs':{"mostly":.95},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}
                },
        ]

        for t in T:
            # out = D3.expect_column_values_to_not_be_null(*t['in'], **t['kwargs'])
            out = D3.expect_column_values_to_be_null(*t['in'], **t['kwargs'])
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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{"column":"x","type_":"int","target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{"column":"x","type_":"string","target_datasource":"numpy"},
                    'out':{'success':False, 'exception_list':[1,2,4], 'exception_index_list':[0,1,2]}},
                {
                    'in':{"column":"y","type_":"float","target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{"column":"y","type_":"float","target_datasource":"numpy"},
                    'out':{'success':False, 'exception_list':[1.0,2.2,5.3], 'exception_index_list':[0,1,2]}},
                {
                    'in':{"column":"z","type_":"string","target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{"column":"b","type_":"boolean","target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}}
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
            out = D.expect_column_values_to_be_of_type(**t['in'])
            self.assertEqual(out, t['out'])

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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{"column":"x","type_list":["int"],"target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{"column":"x","type_list":["string"],"target_datasource":"numpy"},
                    'out':{'success':False, 'exception_list':[1,2,4], 'exception_index_list':[0,1,2]}},
                {
                    'in':{"column":"y","type_list":["float"],"target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{"column":"y","type_list":["float"],"target_datasource":"numpy"},
                    'out':{'success':False, 'exception_list':[1.0,2.2,5.3], 'exception_index_list':[0,1,2]}},
                {
                    'in':{"column":"z","type_list":["string"],"target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{"column":"b","type_list":["boolean"],"target_datasource":"python"},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                   'in':{"column":"s", "type_list":["string", "int"], "target_datasource":"python"},
                   'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                #{
                #    'in':['n','null','python'],
                #    'kwargs':{'mostly':.5},
                #    'out':{'success':True, 'exception_list':[np.nan]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_in_type_list(**t['in'])
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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':['x', [1,2,4]],
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':['x', [4,2]],
                    'out':{'success':False, 'exception_index_list':[0], 'exception_list':[1]}},
                {
                    'in':['y', []],
                    'out':{'success':False, 'exception_index_list':[0,1,2], 'exception_list':[1,2,5]}},
                {
                    'in':['z', ['hello','jello','mello']],
                    'out': {'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':['z', ['hello']],
                    'out': {'success':False, 'exception_index_list':[1,2], 'exception_list':['jello','mello']}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_in_set(*t['in'])
            self.assertEqual(out,t['out'])

        self.assertRaises(
            TypeError,
            D.expect_column_values_to_be_in_set, 'x', None
        )

        D2 = ge.dataset.PandasDataSet({
            'x' : [1,1,2,None],
            'y' : [None,None,None,None],
        })
        D2.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x', 'values_set':[1,2]},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'x', 'values_set':[1]},
                    'out':{'success':False, 'exception_index_list':[2], 'exception_list':[2]}},
                {
                    'in':{'column':'x', 'values_set':[1], 'mostly':.66},
                    'out':{'success':True, 'exception_index_list':[2], 'exception_list':[2]}},
                {
                    'in':{'column':'x', 'values_set':[2], 'mostly':.66},
                    'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':[1,1]}},
                {
                    'in':{'column':'y', 'values_set':[]},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'y', 'values_set':[2], 'mostly':.5},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_be_in_set(**t['in'])
            self.assertEqual(out, t['out'])


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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':['x', [1,2]],'kwargs':{},
                    'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':[1,2]}},
                {
                    'in':['x',[5,6]],'kwargs':{},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':['z',['hello', 'jello']],'kwargs':{},
                    'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':['hello', 'jello']}},
                {
                    'in':['z',[]],'kwargs':{},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':['a', [1]],'kwargs':{},
                    'out':{'success':False, 'exception_index_list':[0,1], 'exception_list':[1, 1]}},
                {
                    'in':['n', [2]],
                    'kwargs':{},
                    'out':{'success':False, 'exception_index_list':[2], 'exception_list':[2]}},
                {
                    'in':['n', []],
                    'kwargs':{},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':['a', [1]], 
                    'kwargs':{'mostly':.1},
                    'out':{'success':True, 'exception_index_list':[0,1], 'exception_list':[1, 1]}},
                {
                    'in':['n', [2]],
                    'kwargs':{'mostly':.9},
                    'out':{'success':False, 'exception_index_list':[2], 'exception_list':[2]}}
        ]

        for t in T:
            out = D.expect_column_values_to_not_be_in_set(*t['in'],**t['kwargs'])
            self.assertEqual(out, t['out'])



    def test_expect_column_values_to_be_between(self):
        """

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'y' : [1, 2, 3, 4, 5, 6, 7, 8, 9, "abc"],
            'z' : [1, 2, 3, 4, 5, None, None, None, None, None],
            'ts' : [
                'Jan 01 1870 12:00:01',
                'Dec 31 1999 12:00:01',
                'Jan 01 2000 12:00:01',
                'Feb 01 2000 12:00:01',
                'Mar 01 2000 12:00:01',
                'Apr 01 2000 12:00:01',
                'May 01 2000 12:00:01',
                'Jun 01 2000 12:00:01',
                None,
                'Jan 01 2001 12:00:01',
            ],
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        self.maxDiff = None

        with open("./tests/test_sets/expect_column_values_to_be_between_test_set_ADJ.json") as f:
            T = json.load(f)
            # print json.dumps(T, indent=2)

        for t in T:
            out = D.expect_column_values_to_be_between(**t['in'])#, **t['kwargs'])
            self.assertEqual(out, t['out'])

    def test_expect_column_value_lengths_to_be_between(self):
        D = ge.dataset.PandasDataSet({
            's1':['smart','silly','sassy','slimy','sexy'],
            's2':['cool','calm','collected','casual','creepy'],
            's3':['cool','calm','collected','casual',None],
            's4':[1,2,3,4,5]
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'s1', 'min_value':3, 'max_value':5},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'s2', 'min_value':4, 'max_value':6},
                    'out':{'success':False, 'exception_index_list':[2], 'exception_list':['collected']}},
                {
                    'in':{'column':'s2', 'min_value':None, 'max_value':10},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'s3', 'min_value':None, 'max_value':10},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}}
        ]

        for t in T:
            out = D.expect_column_value_lengths_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])

        with self.assertRaises(TypeError):
            D.expect_column_value_lengths_to_be_between(**{'column':'s4', 'min_value':None, 'max_value':10})


    def test_expect_column_values_to_match_regex(self):
        """
        Cases Tested:
            Tested mostly alphabet regex
        """

        D = ge.dataset.PandasDataSet({
            'x' : ['aa', 'ab', 'ac', 'a1', None],
            'y' : ['aa', 'ab', 'ac', 'ba', 'ca'],
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")


        D2 = ge.dataset.PandasDataSet({
            'a' : ['aaa', 'abb', 'acc', 'add', 'bee'],
            'b' : ['aaa', 'abb', 'acc', 'bdd', None],
            'c' : [ None,  None,  None,  None, None],
        })
        D2.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x', 'regex':'^a'},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{'column':'x', 'regex':'aa'},
                    'out':{'success':False, 'exception_list':['ab', 'ac', 'a1'], 'exception_index_list':[1,2,3]}},
                {
                    'in':{'column':'x', 'regex':'a[a-z]'},
                    'out':{'success':False, 'exception_list':['a1'], 'exception_index_list':[3]}},
                {
                    'in':{'column':'y', 'regex':'[abc]{2}'},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{'column':'y', 'regex':'[z]'},
                    'out':{'success':False, 'exception_list':['aa', 'ab', 'ac', 'ba', 'ca'], 'exception_index_list':[0,1,2,3,4]}}
        ]

        for t in T:
            out = D.expect_column_values_to_match_regex(**t['in'])
            self.assertEqual(out, t['out'])

        T = [
                {
                    'in':{'column':'a', 'regex':'^a', 'mostly':.9},
                    'out':{'success':False, 'exception_list':['bee'], 'exception_index_list':[4]}},
                {
                    'in':{'column':'a', 'regex':'^a', 'mostly':.8},
                    'out':{'success':True, 'exception_list':['bee'], 'exception_index_list':[4]}},
                {
                    'in':{'column':'a', 'regex':'^a', 'mostly':.7},
                    'out':{'success':True, 'exception_list':['bee'], 'exception_index_list':[4]}},
                {
                    'in':{'column':'b', 'regex':'^a', 'mostly':.9},
                    'out':{'success':False, 'exception_list':['bdd'], 'exception_index_list':[3]}},
                {
                    'in':{'column':'b', 'regex':'^a', 'mostly':.75},
                    'out':{'success':True, 'exception_list':['bdd'], 'exception_index_list':[3]}},
                {
                    'in':{'column':'b', 'regex':'^a', 'mostly':.5},
                    'out':{'success':True, 'exception_list':['bdd'], 'exception_index_list':[3]}},
                {
                    'in':{'column':'c', 'regex':'^a'},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}},
                {
                    'in':{'column':'c', 'regex':'^a', 'mostly':.5},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list':[]}}
        ]

        for t in T:
            out = D2.expect_column_values_to_match_regex(**t['in'])
            self.assertEqual(out, t['out'])



    def test_expect_column_values_to_not_match_regex(self):
        #!!! Need to test mostly and suppress_exceptions

        D = ge.dataset.PandasDataSet({
            'x' : ['aa', 'ab', 'ac', 'a1', None, None, None],
            'y' : ['axxx', 'exxxx', 'ixxxx', 'oxxxxx', 'uxxxxx', 'yxxxxx', 'zxxxx'],
            'z' : [None, None, None, None, None, None, None]
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'x', 'regex':'^a'},
                    'out':{'success':False, 'exception_index_list':[0,1,2,3], 'exception_list':['aa', 'ab', 'ac', 'a1']}},
                {
                    'in':{'column':'x', 'regex':'^b'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'y', 'regex':'^z'},
                    'out':{'success':False, 'exception_index_list':[6], 'exception_list':['zxxxx']}}
        ]

        for t in T:
            out = D.expect_column_values_to_not_match_regex(**t['in'])
            self.assertEqual(out, t['out'])


    def test_expect_column_values_to_match_regex_list(self):
        with open("./tests/test_sets/expect_column_values_to_match_regex_list_test_set.json") as f:
            J = json.load(f)
            D = ge.dataset.PandasDataSet(J["dataset"])
            D.set_default_expectation_argument("output_format", "COMPLETE")
            T = J["tests"]

            self.maxDiff = None

        for t in T:
            out = D.expect_column_values_to_match_regex_list(**t['in'])
            self.assertEqual(out, t['out'])


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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'us_dates', 'strftime_format':'%m/%d/%Y'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'us_dates_type_error','strftime_format':'%m/%d/%Y', 'mostly': 0.5},
                    'out':{'success':True, 'exception_index_list':[2], 'exception_list':[5]}},
                {
                    'in':{'column':'us_dates_type_error','strftime_format':'%m/%d/%Y'},
                    'out':{'success':False,'exception_index_list':[2], 'exception_list':[5]}},
                {
                    'in':{'column':'almost_iso8601','strftime_format':'%Y-%m-%dT%H:%M:%S'},
                    'out':{'success':True,'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'almost_iso8601_val_error','strftime_format':'%Y-%m-%dT%H:%M:%S'},
                    'out':{'success':False,'exception_index_list':[0], 'exception_list':['1977-05-55T00:00:00']}}
        ]

        for t in T:
            out = D.expect_column_values_to_match_strftime_format(**t['in'])
            self.assertEqual(out, t['out'])

    def test_expect_column_values_to_be_dateutil_parseable(self):

        D = ge.dataset.PandasDataSet({
            'c1':['03/06/09','23 April 1973','January 9, 2016'],
            'c2':['9/8/2012','covfefe',25],
            'c3':['Jared','June 1, 2013','July 18, 1976']
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':['c1'],
                    'kwargs':{},
                    'out':{'success':True, 'exception_list':[], 'exception_index_list': []}},
                {
                    'in':['c2'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['covfefe', 25], 'exception_index_list': [1, 2]}},
                {
                    'in':['c3'],
                    'kwargs':{},
                    'out':{'success':False, 'exception_list':['Jared'], 'exception_index_list': [0]}},
                {
                    'in':['c3'],
                    'kwargs':{'mostly':.5},
                    'out':{'success':True, 'exception_list':['Jared'], 'exception_index_list': [0]}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_dateutil_parseable(*t['in'], **t['kwargs'])
            self.assertEqual(out, t['out'])

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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'json_col'},
                    'out':{'success':True, 'exception_index_list':[], 'exception_list':[]}},
                {
                    'in':{'column':'not_json'},
                    'out':{'success':False, 'exception_index_list':[0,1,2,3], 'exception_list':[4,5,6,7]}},
                {
                    'in':{'column':'py_dict'},
                    'out':{'success':False, 'exception_index_list':[0,1,2,3], 'exception_list':[{'a':1, 'out':1},{'b':2, 'out':4},{'c':3, 'out':9},{'d':4, 'out':16}]}},
                {
                    'in':{'column':'most'},
                    'out':{'success':False, 'exception_index_list':[3], 'exception_list':['d4']}},
                {
                    'in':{'column':'most', 'mostly':.75},
                    'out':{'success':True, 'exception_index_list':[3], 'exception_list':['d4']}}
        ]

        for t in T:
            out = D.expect_column_values_to_be_json_parseable(**t['in'])
            self.assertEqual(out, t['out'])

    def test_expect_column_values_to_match_json_schema(self):

        with open("./tests/test_sets/expect_column_values_to_match_json_schema_test_set.json") as f:
            J = json.load(f)
            D = ge.dataset.PandasDataSet(J["dataset"])
            D.set_default_expectation_argument("output_format", "COMPLETE")
            T = J["tests"]

            self.maxDiff = None

        for t in T:
            out = D.expect_column_values_to_match_json_schema(**t['in'])#, **t['kwargs'])
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
            'b' : [True, False],
        })
        D.set_default_expectation_argument("output_format", "BASIC")

        T = [
                {
                    'in':{'column':'x', 'min_value':2, 'max_value':5},
                    'out':{'success':True, 'true_value':3.5}},
                {
                    'in':{'column':'x', 'min_value':1, 'max_value':2},
                    'out':{'success':False, 'true_value':3.5}},
                {
                    'in':{'column':'y', 'min_value':5, 'max_value':5},
                    'out':{'success':True, 'true_value':5}},
                {
                    'in':{'column':'y', 'min_value':4, 'max_value':4},
                    'out':{'success':False, 'true_value':5}},
                {
                    'in':{'column':'z', 'min_value':5, 'max_value':5},
                    'out':{'success':True, 'true_value':5}},
                {
                    'in':{'column':'z', 'min_value':13, 'max_value':14},
                    'out':{'success':False, 'true_value':5}},
                {
                    'in':{'column':'n', 'min_value':0, 'max_value':0},
                    'out':{'success':True, 'true_value':0.0}}
        ]

        for t in T:
            out = D.expect_column_mean_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])


        typedf = ge.dataset.PandasDataSet({
            's' : ['s', np.nan, None, None],
            'b' : [True, False, False, True],
            'x' : [True, None, False, None],
        })
        typedf.set_default_expectation_argument("output_format", "BASIC")

        T = [
                {
                    'in':{'column':'s', 'min_value':0, 'max_value':0},
                    'out':{'success':False, 'true_value':None}},
                {
                    'in':{'column':'b', 'min_value':0, 'max_value':1},
                    'out':{'success':True, 'true_value':0.5}},
                {
                    'in':{'column':'x', 'min_value':0, 'max_value':1},
                    'out':{'success':True, 'true_value':0.5}}
        ]

        for t in T[1:]:
            out = typedf.expect_column_mean_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])

        with self.assertRaises(TypeError):
            typedf.expect_column_mean_to_be_between(T[0]['in'])



    def test_expect_column_stdev_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,1,3],
            'dist2' : [-1,0,1]
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'dist1', 'min_value':.5, 'max_value':1.5},
                    'out':{'success':True, 'true_value':D['dist1'].std()}},
                {
                    'in':{'column':'dist1', 'min_value':2, 'max_value':3},
                    'out':{'success':False, 'true_value':D['dist1'].std()}},
                {
                    'in':{'column':'dist2', 'min_value':2, 'max_value':3},
                    'out':{'success':False, 'true_value':1.0}},
                {
                    'in':{'column':'dist2', 'min_value':0, 'max_value':1},
                    'out':{'success':True, 'true_value':1.0}}
        ]

        for t in T:
            out = D.expect_column_stdev_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])


    def test_expect_column_unique_value_count_to_be_between(self):

        D = ge.dataset.PandasDataSet({
            'dist1' : [1,2,3,4,5,6,7,8],
            'dist2' : [1,2,3,4,5,None,None,None],
            'dist3' : [2,2,2,2,5,6,7,8],
            'dist4' : [1,1,1,1,None,None,None,None]
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{
                        'column': 'dist1',
                        'min_value': 0,
                        'max_value': 10
                    },
                    'kwargs':{},
                    'out':{'success':True, 'true_value': 8}
                },{
                    'in':{
                        "column" : 'dist2',
                        "min_value" : None,
                        "max_value" : None
                    },
                    'kwargs':{},
                    'out':{'success':True, 'true_value': 5}
                },{
                    'in':{
                        "column": 'dist3',
                        "min_value": None,
                        "max_value": 5
                    },
                    'kwargs':{},
                    'out':{'success':True, 'true_value': 5}
                },{
                    'in':{
                        "column": 'dist4',
                        "min_value": 2,
                        "max_value": None
                    },
                    'kwargs':{},
                    'out':{'success':False, 'true_value': 1}
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
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{'column':'dist1', 'min_value':.5, 'max_value':1.5},
                    'out':{'success':True, 'true_value': 2./3}},
                {
                    'in':{'column':'dist1', 'min_value':2, 'max_value':3},
                    'out':{'success':False, 'true_value': 2./3}},
                {
                    'in':{'column':'dist2', 'min_value':2, 'max_value':3},
                    'out':{'success':False, 'true_value':1.0}},
                {
                    'in':{'column':'dist2', 'min_value':0, 'max_value':1},
                    'out':{'success':True, 'true_value':1.0}}
        ]

        for t in T:
            out = D.expect_column_proportion_of_unique_values_to_be_between(**t['in'])
            self.assertEqual(out, t['out'])

    def test_expect_column_values_to_be_increasing(self):
        print("=== test_expect_column_values_to_be_increasing ===")
        with open("./tests/test_sets/expect_column_values_to_be_increasing_test_set.json") as f:
            J = json.load(f)
            D = ge.dataset.PandasDataSet(J["dataset"])
            D.set_default_expectation_argument("output_format", "COMPLETE")
            T = J["tests"]

            self.maxDiff = None

        for t in T:
            print(t)
            out = D.expect_column_values_to_be_increasing(**t['in'])#, **t['kwargs'])
            self.assertEqual(out, t['out'])

    def test_expect_column_values_to_be_decreasing(self):
        print("=== test_expect_column_values_to_be_decreasing ===")
        with open("./tests/test_sets/expect_column_values_to_be_decreasing_test_set.json") as f:
            J = json.load(f)
            D = ge.dataset.PandasDataSet(J["dataset"])
            D.set_default_expectation_argument("output_format", "COMPLETE")
            T = J["tests"]

            self.maxDiff = None

        for t in T:
            print(t)
            out = D.expect_column_values_to_be_decreasing(**t['in'])
            self.assertEqual(out, t['out'])


    def test_expect_column_most_common_value_to_be(self):

        D = ge.dataset.PandasDataSet({
            'x' : [1,1,2,2,3,None, None, None, None, None],
            'y' : ['hello', 'jello', 'mello', 'hello', 'jello', 'mello', 'hello', 'jello', 'mello', 'jello'],
            'z' : [1,2,2,3,3,3,4,4,4,4],
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{"column":"x","value":1},
                    'out':{"success":False, "true_value":[1,2]},
                },{
                    'in':{"column":"x", "value":1, "ties_okay":True},
                    'out':{"success":True, "true_value":[1,2]},
                },{
                    'in':{"column":"x","value":3},
                    'out':{"success":False, "true_value":[1,2]},
                },{
                    'in':{"column":"x","value":3, "ties_okay":True},
                    'out':{"success":False, "true_value":[1,2]},
                },{
                    'in':{"column":"y","value":"jello"},
                    'out':{'success':True, "true_value":["jello"]},
                },{
                    'in':{"column":"y","value":"hello"},
                    'out':{'success':False, "true_value":["jello"]},
                },{
                    'in':{"column":"z","value":4},
                    'out':{'success':True, "true_value":[4]},
                }
        ]

        for t in T:
            out = D.expect_column_most_common_value_to_be(**t['in'])
            self.assertEqual(out, t['out'])


    def test_expect_column_most_common_value_to_be_in_set(self):

        D = ge.dataset.PandasDataSet({
            'x' : [1,1,2,2,3,None, None, None, None, None],
            'y' : ['hello', 'jello', 'mello', 'hello', 'jello', 'mello', 'hello', 'jello', 'mello', 'jello'],
            'z' : [1,2,2,3,3,3,4,4,4,4],
        })
        D.set_default_expectation_argument("output_format", "COMPLETE")

        T = [
                {
                    'in':{"column":"x","value_set":[1]},
                    'out':{"success":False, "true_value":[1,2]},
                },{
                    'in':{"column":"x", "value_set":[1], "ties_okay":True},
                    'out':{"success":True, "true_value":[1,2]},
                },{
                    'in':{"column":"x","value_set":[3]},
                    'out':{"success":False, "true_value":[1,2]},
                },{
                    'in':{"column":"y","value_set":["jello", "hello"]},
                    'out':{'success':True, "true_value":["jello"]},
                },{
                    'in':{"column":"y","value_set":["hello", "mello"]},
                    'out':{'success':False, "true_value":["jello"]},
                },{
                    'in':{"column":"z","value_set":[4]},
                    'out':{'success':True, "true_value":[4]},
                }
        ]


        for t in T:
            out = D.expect_column_most_common_value_to_be_in_set(**t['in'])
            self.assertEqual(out, t['out'])


    def test_expectation_decorator_summary_mode(self):

        df = ge.dataset.PandasDataSet({
            'x' : [1,2,3,4,5,6,7,7,None,None],
        })
        df.set_default_expectation_argument("output_format", "COMPLETE")

        self.maxDiff = None
        self.assertEqual(
            df.expect_column_values_to_be_between('x', min_value=1, max_value=5, output_format="SUMMARY"),
            {
                "success" : False,
                "summary_obj" : {
                    "element_count" : 10,
                    "missing_count" : 2,
                    "missing_percent" : .2,
                    "exception_count" : 3,
                    "partial_exception_counts": {
                        6.0 : 1,
                        7.0 : 2,
                    },
                    "exception_percent": 0.3,
                    "exception_percent_nonmissing": 0.375,
                    "partial_exception_list" : [6.0,7.0,7.0],
                    "partial_exception_index_list": [5,6,7],
                }
            }
        )

        self.assertEqual(
            df.expect_column_mean_to_be_between("x", 3, 7, output_format="SUMMARY"),
            {
                'success': True,
                'true_value': 4.375,
                'summary_obj': {
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
        df.set_default_expectation_argument('output_format', 'COMPLETE')

        self.assertEqual(
            df.expect_column_mean_to_be_between('x',4,6),
            {'success':True, 'true_value':5}
        )

        self.assertEqual(
            df.expect_column_values_to_be_between('y',1,6),
            {'success':False, 'exception_list':[8,10], 'exception_index_list':[3,4]}
        )

        self.assertEqual(
            df.expect_column_values_to_be_between('y',1,6,mostly=.5),
            {'success':True, 'exception_list':[8,10], 'exception_index_list':[3,4]}
        )

        self.assertEqual(
            df.expect_column_values_to_be_in_set('z',['a','b','c']),
            {'success':False, 'exception_list':['abc'], 'exception_index_list':[4]}
        )

        self.assertEqual(
            df.expect_column_values_to_be_in_set('z',['a','b','c'],mostly=.5),
            {'success':True, 'exception_list':['abc'], 'exception_index_list':[4]}
        )




if __name__ == "__main__":
    unittest.main()
