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
df.expect_column_values_to_be_in_set("D", ['e','f','g','x'])

sam = df.sample(n=2)

# print(type(sam))
# print(sam.find_expectations())
# print(sam.validate(only_return_failures=True))
#
#
# df.discard_failing_expectations = True
#
# sam = df.sample(frac=0.25, replace=True)
# print(sam)
# print(type(sam))
# print(sam.find_expectations())
# print(sam.validate(only_return_failures=True))
