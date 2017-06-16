import unittest
import json
import hashlib
import datetime
import numpy as np

import great_expectations as ge

class TestStringMethods(unittest.TestCase):

    def test_expect_column_values_to_be_in_set(self):
        """
        Cases Tested:

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })

        tests = [
            {'input':('x', [1,2,4]), 'kwargs': {}, 'output':{'success':True, 'exception_list':[]}},
            {'input':('x', [4,2]), 'kwargs': {}, 'output':{'success':False, 'exception_list':[1]}},
            {'input':('y', []), 'kwargs': {}, 'output':{'success':False, 'exception_list':[1,2,5]}},
            {'input':('z', ['hello','jello','mello']), 'kwargs': {}, 'output': {'success':True, 'exception_list':[]}},
            {'input':('z', ['hello']), 'kwargs': {}, 'output': {'success':False, 'exception_list':['jello','mello']}}
        ]

        for t in tests:
            out = D.expect_column_values_to_be_in_set(*t['input'], **t['kwargs'])
            self.assertEqual(out,t['output'])

    def test_expect_column_values_to_be_unique(self):
        """
        Cases Tested:
            Different data types are not equal
            Different values are not equal
            None and np.nan values trigger True
        """

        D = ge.dataset.PandasDataSet({
            'a' : ['2', '2'],
            'b' : [1, '2'],
            'c' : [1, 1],
            'd' : [1, '1'],
            'n' : [None, np.nan]
        })

        #Column string values are equal - 2 and 2
        self.assertEqual(D.expect_column_values_to_be_unique('a'), {'success':False, 'exception_list':['2']})

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

        D2 = ge.dataset.PandasDataSet({
            'a' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'b' : [1, 2, 3, 4, 5, 6, 7, 8, 9, None],
        })

        self.assertEqual(
            D.expect_column_values_to_not_be_null('x'),
            {'success':False, 'exception_list':[None]}
        )




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

        # Test on np.an and None
        # Test exceptions (not_null values) show up properly
        self.assertEqual(
                D.expect_column_values_to_be_null('x'),
                {'success':False, 'exception_list':[2,2]}
            )

    def test_expect_column_mean_to_be_between(self):
        """
        #!!! Ignores null (None and np.nan) values. If all null values, return (False, None)
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

        #[2, 5]
        self.assertEqual(
            D.expect_column_mean_to_be_between('x', 2, 5)['success'],
            True #(True, 3.5)
            )


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


        #!!! Why do these tests have verbose=True?
        #['aa', 'ab', 'ac', 'a1', None]
        self.assertEqual(
            D.expect_column_values_to_match_regex('x', '^a'),
            {'success':True,'exception_list':[]}
            )

    def test_expect_column_values_match_strftime_format(self):
        """
        Cases Tested:


        !!! TODO: Add tests for input types and raised exceptions

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1,2,4],
            'us_dates' : ['4/30/2017','4/30/2017','7/4/1776'],
            'us_dates_type_error' : ['4/30/2017','4/30/2017', 5],
            'almost_iso8601' : ['1977-05-25T00:00:00', '1980-05-21T13:47:59', '2017-06-12T23:57:59'],
            'almost_iso8601_val_error' : ['1977-05-55T00:00:00', '1980-05-21T13:47:59', '2017-06-12T23:57:59']
        })

        tests = [
                {'input':('us_dates','%m/%d/%Y'), 'kwargs':{'mostly': None}, 'success':True, 'exception_list':[]},
                {'input':('us_dates_type_error','%m/%d/%Y'), 'kwargs':{'mostly': 0.5}, 'success':True, 'exception_list':[5]},
                {'input':('us_dates_type_error','%m/%d/%Y'), 'kwargs':{'mostly': None}, 'success':False,'exception_list':[5]},
                {'input':('almost_iso8601','%Y-%m-%dT%H:%M:%S'), 'kwargs':{'mostly': None}, 'success':True,'exception_list':[]},
                {'input':('almost_iso8601_val_error','%Y-%m-%dT%H:%M:%S'), 'kwargs':{'mostly': None}, 'success':False,'exception_list':['1977-05-55T00:00:00']}
                ]

        for t in tests:
            out = D.expect_column_values_to_match_strftime_format(*t['input'],**t['kwargs'])
            self.assertEqual( out['success'], t['success'])
            self.assertEqual( out['exception_list'],  t['exception_list'])



    def test_expect_column_values_to_not_match_regex(self):
        #!!! Need to test mostly and suppress_exceptions

        D = ge.dataset.PandasDataSet({
            'x' : ['aa', 'ab', 'ac', 'a1', None, None, None],
            'y' : ['axxx', 'exxxx', 'ixxxx', 'oxxxxx', 'uxxxxx', 'yxxxxx', 'zxxxx'],
            'z' : [None, None, None, None, None, None, None]
        })

        self.assertEqual(
            D.expect_column_values_to_not_match_regex('x', '^a'),
            {'success':False, 'exception_list':['aa', 'ab', 'ac', 'a1']}
            )

    def test_expect_column_values_to_be_between(self):
        """

        """

        D = ge.dataset.PandasDataSet({
            'x' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'y' : [1, 2, 3, 4, 5, 6, 7, 8, 9, "abc"],
            'z' : [1, 2, 3, 4, 5, None, None, None, None, None],
        })

        self.assertEqual(
            D.expect_column_values_to_be_between('x', 1, 10),
            {'success':True, 'exception_list':[]}
        )



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

        self.assertEqual( D.expect_column_values_to_not_be_in_set('x',[1,2]),
            {'success':False, 'exception_list':[1,2]}
            )

    def test_expect_table_row_count_to_be_between(self):
        D = ge.dataset.PandasDataSet({'c1':[4,5,6,7],'c2':['a','b','c','d'],'c3':[None,None,None,None]})

        out1 = D.expect_table_row_count_to_be_between(3,5)
        self.assertEqual( out1['success'], True)
        self.assertEqual( out1['true_row_count'], 4)


    def test_expect_table_row_count_to_equal(self):
        D = ge.dataset.PandasDataSet({'c1':[4,5,6,7],'c2':['a','b','c','d'],'c3':[None,None,None,None]})

        out1 = D.expect_table_row_count_to_equal(4)
        self.assertEqual( out1['success'], True)
        self.assertEqual( out1['true_row_count'], 4)


    def test_expect_column_value_lengths_to_be_between(self):
        s1 = ['smart','silly','sassy','slimy','sexy']
        s2 = ['cool','calm','collected','casual','creepy']
        D = ge.dataset.PandasDataSet({'s1':s1,'s2':s2})
        out1 = D.expect_column_value_lengths_to_be_between('s1', min_value=3, max_value=5)
        out2 = D.expect_column_value_lengths_to_be_between('s2', min_value=4, max_value=6)
        out3 = D.expect_column_value_lengths_to_be_between('s2', min_value=None, max_value=10)
        self.assertEqual( out1['success'], True)
        self.assertEqual( out2['success'], False)
        self.assertEqual( len(out2['exception_list']), 1)
        self.assertEqual( out3['success'], True)



    def test_expect_column_values_to_match_regex_list(self):
        pass


    def test_expect_column_values_to_be_dateutil_parseable(self):
        dates = ['03/06/09','23 April 1973','January 9, 2016']
        other = ['197234567','covfefe',25]
        D = ge.dataset.PandasDataSet({'dates':dates, 'other':other})
        out = D.expect_column_values_to_be_dateutil_parseable('dates')
        out2 = D.expect_column_values_to_be_dateutil_parseable('other')
        self.assertEqual( out['success'] , True)
        self.assertEqual( len(out['exception_list']), 0)
        self.assertEqual( out2['success'] , False)
        self.assertEqual( len(out2['exception_list']) , 3)


    def test_expect_column_values_to_be_valid_json(self):
        d1 = json.dumps({'i':[1,2,3],'j':35,'k':{'x':'five','y':5,'z':'101'}})
        d2 = json.dumps({'i':1,'j':2,'k':[3,4,5]})
        D = ge.dataset.PandasDataSet({'json_col':[d1,d2]})
        out = D.expect_column_values_to_be_valid_json('json_col')
        self.assertEqual(out['success'], True)


    def test_expect_column_stdev_to_be_between(self):
        D = ge.dataset.PandasDataSet({'randn':np.random.randn(100)})
        out1 = D.expect_column_stdev_to_be_between('randn',.5,1.5)
        out2 = D.expect_column_stdev_to_be_between('randn',2,3)
        self.assertEqual(out1['success'], True)
        self.assertEqual(out2['success'], False)


    def test_expect_two_column_values_to_be_subsets(self):
        A = [0,1,2,3,4,3,2,1,0]
        B = [2,3,4,5,6,5,4,3,2]
        C = [0,1,2,3,4,5,6,7,8]
        D = ge.dataset.PandasDataSet({'A':A,'B':B,'C':C})
        out1 = D.expect_two_column_values_to_be_subsets('A','C')
        out2 = D.expect_two_column_values_to_be_subsets('A','B',mostly=.5)

        self.assertEqual(out1['success'], True)
        self.assertEqual(out2['success'], True)
        self.assertEqual(out2['not_in_subset'], set([0,1,5,6]))


    def test_expect_two_column_values_to_be_many_to_one(self):
        pass

if __name__ == '__main__':
    unittest.main()
