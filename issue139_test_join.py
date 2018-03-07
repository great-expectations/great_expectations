import great_expectations as ge

df1 = ge.dataset.PandasDataSet({
    'A': ['A0', 'A1', 'A2'],
    'B': ['B0', 'B1', 'B2']},
    index=['K0', 'K1', 'K2'])

df1.expect_column_values_to_match_regex('A', '^A[0-2]$')
df1.expect_column_values_to_match_regex('B', '^B[0-2]$')

df2 = ge.dataset.PandasDataSet({
    'C': ['C0', 'C2', 'C3'],
    'D': ['C0', 'D2', 'D3']},
    index=['K0', 'K2', 'K3'])

df2.expect_column_values_to_match_regex('C', '^C[0-2]$')
df2.expect_column_values_to_match_regex('D', '^D[0-2]$')

# print("-------------------------------------------------------------------")
# print("constituents")
# print(df1)
# print(df1.find_expectations())
# print(df2)
# print(df2.find_expectations())
#
df = df1.join(df2)
#
# print("-------------------------------------------------------------------")
# print("left join")
# print(df)
# print(type(df))
# print(df.find_expectations())
#
# df = df1.join(df2, how='right')
#
# print("-------------------------------------------------------------------")
# print("right join")
# print(df)
# print(type(df))
# print(df.find_expectations())
#
# df = df1.join(df2, how='inner')
#
# print("-------------------------------------------------------------------")
# print("inner join")
# print(df)
# print(type(df))
# print(df.find_expectations())
#
# df = df1.join(df2, how='outer')
#
# print("-------------------------------------------------------------------")
# print("outer join")
# print(df)
# print(type(df))
# print(df.find_expectations())

