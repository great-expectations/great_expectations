import json
import hashlib
import datetime
import numpy as np

from nose.tools import *
import great_expectations as ge

def test_expect_column_values_to_be_in_set():
    """
    Cases Tested:
        
    """

    D = ge.dataset.PandasDataSet({
        'x' : [1,2,4],
        'y' : [1,2,5],
        'z' : ['hello', 'jello', 'mello'],
    })

    tests = [{'input':('x',[1,2,4]), 'result':{'success':True,'exception_list':[]}}]
    #1: ('x',[1,2,4]),
    #2: ('x',[4,2]),
    #3: ('y',[]),
    #4: ('z',['hello', 'jello', 'mello']),
    #5: ('z',['hello'])

    # 1: {'success': True, 'exception_list':[]},
    # 2: {'success': False, 'exception_list':[1]},
    # 3: {'success': False, 'exception_list':[1,2,5]},
    # 4: {'success': True, 'exception_list':[]},
    # 5: {'success': False, 'exception_list':['jello','mello']}
    # }

    for i in test_inputs.keys():
        col,vals = test_inputs[i]
        result = D.expect_column_values_to_be_in_set(col,vals)
        assert result['success']==test_outputs[i]['success']
        assert result['result']['exception_list']==test_outputs[i]['exception_list']
    #assert D.expect_column_values_to_be_in_set('x',[1,2,4])==(True, [])
    #assert D.expect_column_values_to_be_in_set('x',[4,2])==(False, [1])
    #assert D.expect_column_values_to_be_in_set('y',[])==(False, [1,2,5])
    #assert D.expect_column_values_to_be_in_set('z',['hello', 'jello', 'mello'])==(True, [])
    #assert D.expect_column_values_to_be_in_set('z',['hello'])==(False, ['jello', 'mello'])

    D2 = ge.dataset.PandasDataSet({
        'x' : [1,1,2,None],
        'y' : [None,None,None,None],
    })

    assert D2.expect_column_values_to_be_in_set('x',[1,2])==(True, [])
    assert D2.expect_column_values_to_be_in_set('x',[1])==(False, [2])
    assert D2.expect_column_values_to_be_in_set('x',[2])==(False, [1, 1])
    assert D2.expect_column_values_to_be_in_set('x',[2], suppress_exceptions=True)==(False, None)
    assert D2.expect_column_values_to_be_in_set('x',[1], mostly=.66)==(True, [2])
    assert D2.expect_column_values_to_be_in_set('x',[1], mostly=.33)==(True, [2])

    assert D2.expect_column_values_to_be_in_set('x',[2], mostly=.66)
    assert D2.expect_column_values_to_be_in_set('x',[2], mostly=.9)==(False, [1,1])

    assert D2.expect_column_values_to_be_in_set('y',[2])==(True, [])
    assert D2.expect_column_values_to_be_in_set('y',[])==(True, [])
    assert D2.expect_column_values_to_be_in_set('y',[2], mostly=.5)==(True, [])
    assert D2.expect_column_values_to_be_in_set('y',[], mostly=.5)==(True, [])

    #print json.dumps(
    #    D2.ge_config,
    #    indent=2
    #)

    # assert 0

def test_expect_column_values_to_be_equal_across_columns():
    """
    Cases Tested:
        Column values in x and y are equal
        Column values in x and z are not equal
        Column values in a and z are not equal
    """

    D = ge.dataset.PandasDataSet({
        'x' : [2, 5, 8],
        'y' : [2, 5, 8],
        'z' : [2, 5, 6],
        'a' : [1, 2, 3],
    })

    #Test True case for col x==y
    assert D.expect_column_values_to_be_equal_across_columns('x', 'y', suppress_exceptions=True)==(True, None)
    #Test one value False case for col x==z
    assert D.expect_column_values_to_be_equal_across_columns('x', 'z', suppress_exceptions=True)==(False, None)
    #Test True 
    assert D.expect_column_values_to_be_equal_across_columns('a', 'z', suppress_exceptions=True)==(False, None)

def test_expect_column_values_to_be_unique():
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
    assert D.expect_column_values_to_be_unique('a') == (False, ['2'])
    #Column values are not equal - 1 and '2'
    assert D.expect_column_values_to_be_unique('b') == (True, [])
    #Column int values are equal - 1 and '2'
    assert D.expect_column_values_to_be_unique('c') == (False, [1])
  
    #!!! Different data types are never equal
    #Column int value and string value are equal - 1 and '1'
    assert D.expect_column_values_to_be_unique('d') == (True, [])

    #np.nan and None pass
    assert D.expect_column_values_to_be_unique('n') == (True, [])

    # Test suppress_exceptions
    assert D.expect_column_values_to_be_unique('n',suppress_exceptions = True) == (True, [])
    assert D.expect_column_values_to_be_unique('a',suppress_exceptions = True) == (False, None)

    df = ge.dataset.PandasDataSet({
        'a' : ['2', '2', '2', '2'],
        'b' : [1, '2', '2', '3'],
        'n' : [None, None, np.nan, None],
    })

    #Column string values are equal - 2 and 2
    assert df.expect_column_values_to_be_unique('a') == (False, ['2','2','2'])

    # Test mostly
    # !!! Really important to remember that this is mostly for NOT-NULL values.
    # !!! Tricky because you have to keep that in your mind if your column has many nulls
    assert df.expect_column_values_to_be_unique('b', mostly=.25)==(True, ['2'])
    assert df.expect_column_values_to_be_unique('b', mostly=.75)==(False, ['2'])
    assert df.expect_column_values_to_be_unique('a', mostly=1)==(False, ['2','2','2'])
    assert df.expect_column_values_to_be_unique('n', mostly=.2)==(True, [])

    # Test suppress_exceptions once more
    assert df.expect_column_values_to_be_unique('a',suppress_exceptions = True) == (False, None)


def test_expect_column_values_to_not_be_null():
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

    assert D.expect_column_values_to_not_be_null('x')==(False, [None])
    assert D.expect_column_values_to_not_be_null('y')==(False, [None])
    assert D.expect_column_values_to_not_be_null('n')==(False, [None, None])
    assert D.expect_column_values_to_not_be_null('z')==(True, [])

    assert D.expect_column_values_to_not_be_null('x', suppress_exceptions=True)==(False, None)
    assert D.expect_column_values_to_not_be_null('n', suppress_exceptions=True)==(False, None)
    assert D.expect_column_values_to_not_be_null('z', suppress_exceptions=True)==(True, None)

    assert D2.expect_column_values_to_not_be_null('a')==(True, [])
    assert D2.expect_column_values_to_not_be_null('a', mostly=.90)==(True, [])
    assert D2.expect_column_values_to_not_be_null('b')==(False, [None])
    assert D2.expect_column_values_to_not_be_null('b', mostly=.95)==(False, [None])
    assert D2.expect_column_values_to_not_be_null('b', mostly=.90)==(True, [None])

    assert D2.expect_column_values_to_not_be_null('b', suppress_exceptions=True)==(False, None)
    assert D2.expect_column_values_to_not_be_null('b', mostly=.95, suppress_exceptions=True)==(False, None)
    assert D2.expect_column_values_to_not_be_null('b', mostly=.90, suppress_exceptions=True)==(True, None)



def test_expect_column_values_to_be_null():
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
    assert D.expect_column_values_to_be_null('x')==(False, [2,2])
    assert D.expect_column_values_to_be_null('y')==(False, [2,2])
    assert D.expect_column_values_to_be_null('z')==(False, [2,5,7])
    assert D.expect_column_values_to_be_null('a')==(True, [])

    # Test suppress_exceptions
    assert D.expect_column_values_to_be_null('x', suppress_exceptions = True)==(False, None)
  
    # Test mostly
    assert D.expect_column_values_to_be_null('x', mostly = .2, suppress_exceptions = True)==(True, None)
    assert D.expect_column_values_to_be_null('x', mostly = .8, suppress_exceptions = True)==(False, None)

    assert D.expect_column_values_to_be_null('a', mostly = .5, suppress_exceptions = True)==(True, None)


def test_expect_column_mean_to_be_between():
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
    assert D.expect_column_mean_to_be_between('x', 2, 5)==(True, 3.5)
    assert D.expect_column_mean_to_be_between('x', 1, 2)==(False, 3.5)

    #[5, 5]
    assert D.expect_column_mean_to_be_between('y', 5, 5)==(True, 5)
    assert D.expect_column_mean_to_be_between('y', 4, 4)==(False, 5)

    #[0, 10]
    assert D.expect_column_mean_to_be_between('z', 5, 5)==(True, 5)
    assert D.expect_column_mean_to_be_between('z', 13, 14)==(False, 5)

    #[0, np.nan]
    assert D.expect_column_mean_to_be_between('n', 0, 0)==(True, 0.0)

    typedf = ge.dataset.PandasDataSet({
        's' : ['s', np.nan, None, None],
        'b' : [True, False, False, True],
        'x' : [True, None, False, None],
    })

    # Check TypeError
    assert typedf.expect_column_mean_to_be_between('s', 0, 0)==(False, None)
    assert typedf.expect_column_mean_to_be_between('b', 0, 1)==(True, 0.5)
    assert typedf.expect_column_mean_to_be_between('x', 0, 1)==(True, 0.5)


def test_expect_column_values_to_match_regex():
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
    assert D.expect_column_values_to_match_regex('x', '^a', verbose=True)==(True,[])
    assert D.expect_column_values_to_match_regex('x', 'aa', verbose=True)==(False,['ab', 'ac', 'a1'])
    assert D.expect_column_values_to_match_regex('x', 'a[a-z]', verbose=True)==(False,['a1'])

    #['aa', 'ab', 'ac', 'ba', 'ca']
    assert D.expect_column_values_to_match_regex('y', '[abc]{2}', verbose=True)==(True,[])
    assert D.expect_column_values_to_match_regex('y', '[z]', verbose=True)==(False,['aa', 'ab', 'ac', 'ba', 'ca'])


    assert D.expect_column_values_to_match_regex('y', '[abc]{2}', suppress_exceptions=True) == (True, None)
    assert D.expect_column_values_to_match_regex('y', '[z]', suppress_exceptions=True) == (False, None)

    assert D2.expect_column_values_to_match_regex('a', '^a', mostly=.9) == (False, ['bee'])
    assert D2.expect_column_values_to_match_regex('a', '^a', mostly=.8) == (True, ['bee'])
    assert D2.expect_column_values_to_match_regex('a', '^a', mostly=.7) == (True, ['bee'])

    assert D2.expect_column_values_to_match_regex('b', '^a', mostly=.9) == (False, ['bdd'])
    assert D2.expect_column_values_to_match_regex('b', '^a', mostly=.75) == (True, ['bdd'])
    assert D2.expect_column_values_to_match_regex('b', '^a', mostly=.5) == (True, ['bdd'])

    assert D2.expect_column_values_to_match_regex('b', '^a', mostly=.9, suppress_exceptions=True) == (False, None)
    assert D2.expect_column_values_to_match_regex('b', '^a', mostly=.75, suppress_exceptions=True) == (True, None)
    assert D2.expect_column_values_to_match_regex('b', '^a', mostly=.5, suppress_exceptions=True) == (True, None)

    #Testing for all-null columns
    assert D2.expect_column_values_to_match_regex('c', '^a') == (True, [])
    assert D2.expect_column_values_to_match_regex('c', '^a', mostly=.5) == (True, [])
    assert D2.expect_column_values_to_match_regex('c', '^a', suppress_exceptions=True) == (True, [])
    assert D2.expect_column_values_to_match_regex('c', '^a', mostly=.5, suppress_exceptions=True) == (True, [])


def test_expect_column_values_to_not_match_regex():
    #!!! Need to test mostly and suppress_exceptions

    D = ge.dataset.PandasDataSet({
        'x' : ['aa', 'ab', 'ac', 'a1', None, None, None],
        'y' : ['axxx', 'exxxx', 'ixxxx', 'oxxxxx', 'uxxxxx', 'yxxxxx', 'zxxxx'],
        'z' : [None, None, None, None, None, None, None]
    })

    assert D.expect_column_values_to_not_match_regex('x', '^a') == (False, ['aa', 'ab', 'ac', 'a1'])
    assert D.expect_column_values_to_not_match_regex('x', '^b') == (True, [])

    assert D.expect_column_values_to_not_match_regex('y', '^z') == (False, ['zxxxx'])
    assert D.expect_column_values_to_not_match_regex('y', '^z', mostly=.5) == (True, ['zxxxx'])

    assert D.expect_column_values_to_not_match_regex('x', '^a', suppress_exceptions=True) == (False, None)
    assert D.expect_column_values_to_not_match_regex('x', '^b', suppress_exceptions=True) == (True, None)
    assert D.expect_column_values_to_not_match_regex('y', '^z', suppress_exceptions=True) == (False, None)
    assert D.expect_column_values_to_not_match_regex('y', '^z', mostly=.5, suppress_exceptions=True) == (True, None)

    assert D.expect_column_values_to_match_regex('z', '^a') == (True, [])
    assert D.expect_column_values_to_match_regex('z', '^a', mostly=.5) == (True, [])
    assert D.expect_column_values_to_match_regex('z', '^a', suppress_exceptions=True) == (True, [])
    assert D.expect_column_values_to_match_regex('z', '^a', mostly=.5, suppress_exceptions=True) == (True, [])


def test_expect_column_values_to_be_between():
    """
      
    """

    D = ge.dataset.PandasDataSet({
        'x' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'y' : [1, 2, 3, 4, 5, 6, 7, 8, 9, "abc"],
        'z' : [1, 2, 3, 4, 5, None, None, None, None, None],
    })

    assert D.expect_column_values_to_be_between('x', 1, 10) == (True, [])
    assert D.expect_column_values_to_be_between('x', 0, 20) == (True, [])
    assert D.expect_column_values_to_be_between('x', 1, 9) == (False, [10])
    assert D.expect_column_values_to_be_between('x', 3, 10) == (False, [1, 2])

    assert D.expect_column_values_to_be_between('x', 1, 10, suppress_exceptions=True) == (True, None)
    assert D.expect_column_values_to_be_between('x', 0, 20, suppress_exceptions=True) == (True, None)
    assert D.expect_column_values_to_be_between('x', 1, 9, suppress_exceptions=True) == (False, None)
    assert D.expect_column_values_to_be_between('x', 3, 10, suppress_exceptions=True) == (False, None)

    assert D.expect_column_values_to_be_between('x', 1, 10, mostly=.9) == (True, [])
    assert D.expect_column_values_to_be_between('x', 0, 20, mostly=.9) == (True, [])
    assert D.expect_column_values_to_be_between('x', 1, 9, mostly=.9) == (True, [10])
    assert D.expect_column_values_to_be_between('x', 3, 10, mostly=.9) == (False, [1, 2])

    assert D.expect_column_values_to_be_between('y', 1, 10, mostly=.95) == (False, ["abc"])
    assert D.expect_column_values_to_be_between('y', 1, 10, mostly=.9) == (True, ["abc"])
    assert D.expect_column_values_to_be_between('y', 1, 10, mostly=.8) == (True, ["abc"])

    assert D.expect_column_values_to_be_between('z', 1, 4, mostly=.9) == (False, [5])
    assert D.expect_column_values_to_be_between('z', 1, 4, mostly=.8) == (True, [5])



def test_expect_column_values_to_not_be_in_set():
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

    assert D.expect_column_values_to_not_be_in_set('x',[1,2])==(False, [1,2])
    assert D.expect_column_values_to_not_be_in_set('x',[5,6])==(True, [])
    assert D.expect_column_values_to_not_be_in_set('z',['hello', 'jello'])==(False, ['hello', 'jello'])
    assert D.expect_column_values_to_not_be_in_set('z',[])==(True, [])

    # Test if False exceptions list returns repeat values
    # assert D.expect_column_values_to_not_be_in_set('a', [1]) == (False, [1])
    assert D.expect_column_values_to_not_be_in_set('a', [1]) == (False, [1, 1])

    # Test nonmissing values support
    assert D.expect_column_values_to_not_be_in_set('n', [2]) == (False, [2])
    assert D.expect_column_values_to_not_be_in_set('n', []) == (True, [])

    # Test suppress_exceptions
    assert D.expect_column_values_to_not_be_in_set('n', [2], suppress_exceptions=True) == (False, None)

    # Test mostly
    assert D.expect_column_values_to_not_be_in_set('a', [1], mostly=.2, suppress_exceptions=True) == (True, None)
    assert D.expect_column_values_to_not_be_in_set('n', [2], mostly=1) == (False, [2])



#def test_expect_column_values_to_be_of_type():
#    """
#    Cases Tested:
#      
#    """
#
#    D = ge.dataset.PandasDataSet({
#        'x' : [1,2,4],
#        'y' : [1.0,2.2,5.3],
#        'z' : ['hello', 'jello', 'mello'],
#        'n' : [None, np.nan, None],
#        'b' : [False, True, False],
#        's' : ['hello', 'jello', 1],
#        's1' : ['hello', 2.0, 1],
#    })
#
#    assert D.expect_column_values_to_be_of_type('x','double precision')==(True, [])
#    assert D.expect_column_values_to_be_of_type('x','text')==(False, [1,2,4])
#    assert D.expect_column_values_to_be_of_type('y','double precision')==(True, [])
#    assert D.expect_column_values_to_be_of_type('y','boolean')==(False, [1.0,2.2,5.3])
#    assert D.expect_column_values_to_be_of_type('z','text')==(True, [])
#    assert D.expect_column_values_to_be_of_type('b','boolean')==(True, [])
#    assert D.expect_column_values_to_be_of_type('b','boolean')==(True, [])
#    assert D.expect_column_values_to_be_of_type('n','boolean')==(True, [])
#    assert D.expect_column_values_to_be_of_type('n','text')==(True, [])
#    assert D.expect_column_values_to_be_of_type('n','double precision')==(True, [])
#    assert D.expect_column_values_to_be_of_type('n','double precision')==(True, [])
#
#    assert D.expect_column_values_to_be_of_type('x','crazy type you\'ve never heard of')==(True, [])
#    # Test suppress_exceptions and mostly
#    assert D.expect_column_values_to_be_of_type('s','text', suppress_exceptions=True, mostly=.4)==(True, None)
#    assert D.expect_column_values_to_be_of_type('s','text', suppress_exceptions=False, mostly=.4)==(True, [1])
#    assert D.expect_column_values_to_be_of_type('s1','text', suppress_exceptions=False, mostly=.2)==(True, [2.0 ,1])
#    assert D.expect_column_values_to_be_of_type('s1','double precision', suppress_exceptions=False, mostly=.2)==(True, ['hello'])




def test_expect_column_value_lengths_to_be_less_than_or_equal_to():
    """
    Cases Tested:
      
    """

    D = ge.dataset.PandasDataSet({
        'x' : [1,2,4],
        'y' : ['three','four','five'],
        'n' : [None,np.nan,None],
    })

    assert D.expect_column_value_lengths_to_be_less_than_or_equal_to('x',6)==(True, [])
    assert D.expect_column_value_lengths_to_be_less_than_or_equal_to('y',2)==(False, ['three','four','five'])
    assert D.expect_column_value_lengths_to_be_less_than_or_equal_to('y',6)==(True, [])
    assert D.expect_column_value_lengths_to_be_less_than_or_equal_to('n',0)==(True, [])


#def test_expect_table_row_count_to_be_between(self):
#    pass
#
#def test_expect_table_row_count_to_equal(self):
#    pass
#
#def test_expect_column_value_lengths_to_be_between(self):
#    pass
#
#def test_expect_column_values_to_match_regex_list(self):
#    pass
#
#def test_expect_column_values_to_be_dateutil_parseable(self):
#    pass
#
#def test_expect_column_values_to_be_valid_json(self):
#    pass
#
#def test_expect_column_stdev_to_be_between(self):
#    pass
#
#def test_expect_two_column_values_to_be_subsets(self):
#    pass
#
#def test_expect_two_column_values_to_be_many_to_one(self):
#    pass




