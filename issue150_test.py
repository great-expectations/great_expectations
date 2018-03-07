import great_expectations as ge

print("Create panda")
df = ge.dataset.PandasDataSet({
    'A': [1, 2, 3, 4],
    'B': [5, 6, 7, 8],
    'C': ['a', 'b', 'c', 'd'],
    'D': ['e', 'f', 'g', 'h']
})

print(df)

df.expect_column_values_to_be_in_set("A", [1,2,3,4])
df.expect_column_values_to_be_in_set("B", [5,6,7,8])
df.expect_column_values_to_be_in_set("C", ['a','b','c','d'])
df.expect_column_values_to_be_in_set("D", ['e','f','g','h'])

df.expect_column_values_to_match_regex("A", '^[0-3]+$')

print(df.validate(only_return_failures=True))

print(df.to_records())

