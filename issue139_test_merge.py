import great_expectations as ge

df1 = ge.dataset.PandasDataSet({
    'id': [1, 2, 3, 4, 5],
    'name': ['a', 'b', 'c', 'd', 'e']
})

df1.expect_column_values_to_match_regex('name', '^[A-Za-z ]+$')

df2 = ge.dataset.PandasDataSet({
    'id': [1, 2, 3, 4, 6],
    'salary': [57000, 52000, 59000, 65000, 100000]
})

df2.expect_column_values_to_match_regex('salary', '^[0-9]{4,6]$')

#print(df1)

#print(df2)

#print(df1.find_expectations())

#print(df2.find_expectations())

df = df1.merge(df2, on='id')

# print("-------------------------------------------------------------------")
# print("inner merge")
# print(df)
# print(type(df))
# print(df.find_expectations())
#
# df = df1.merge(df2, on='id', how='outer')
#
# print("-------------------------------------------------------------------")
# print("outer merge")
# print(df)
# print(type(df))
# print(df.find_expectations())
#
# df = df1.merge(df2, on='id', how='left')
#
# print("-------------------------------------------------------------------")
# print("left merge")
# print(df)
# print(type(df))
# print(df.find_expectations())
#
# df = df1.merge(df2, on='id', how='right')
#
# print("-------------------------------------------------------------------")
# print("right merge")
# print(df)
# print(type(df))
# print(df.find_expectations())

