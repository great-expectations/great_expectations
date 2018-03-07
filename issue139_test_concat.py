import great_expectations as ge

import pandas as pd

df1 = ge.dataset.PandasDataSet({
    'A': ['A0', 'A1', 'A2'],
    'B': ['B0', 'B1', 'B2']
})

df1.expect_column_values_to_match_regex('A', '^A[0-2]$')
df1.expect_column_values_to_match_regex('B', '^B[0-2]$')

df2 = ge.dataset.PandasDataSet({
    'A': ['A3', 'A4', 'A5'],
    'B': ['B3', 'B4', 'B5']
})

df2.expect_column_values_to_match_regex('A', '^A[3-5]$')
df2.expect_column_values_to_match_regex('B', '^B[3-5]$')

df3 = ge.dataset.PandasDataSet({
    'C': ['C0', 'C1', 'C2'],
    'D': ['D0', 'D1', 'D2']
})

df3.expect_column_values_to_match_regex('C', '^C[0-2]$')
df3.expect_column_values_to_match_regex('D', '^D[0-2]$')

df12 = pd.concat([df1, df2])

#print(df12)

#print(type(df12))

#print(df12.find_expectations())

#df13 = pd.concat([df1, df3], axis=1)

#print(df13)

#print(type(df13))

#print(df13.find_expectations())


