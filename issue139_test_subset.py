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

# All rows, but columns 'A' and 'D' only
slice1 = df[['A', 'D']]

# # All rows, but column 'A' only
# slice2 = df[['A']]
#
# # First 3 rows. NB Python uses 0-basing.
# slice3 = df[:3]
#
# # Row 2. NB Python uses 0-basing.
# slice4 = df[1:2]
#
# # All rows but last
# slice5 = df[:-1]
#
# # Last row only
# slice6 = df[-1:]
#
# df.discard_failing_expectations = True
# print(df.discard_failing_expectations)
#
# # First 3 rows with second through fourth columns only
# slice7 = df.iloc[:3, 1:4]
#
# # All rows, but only columns 'A' through 'B'
# slice8 = df.loc[0:,'A':'B']
#
# print(df)
#
# print("isinstance check on df:     ", isinstance(df, ge.dataset.PandasDataSet))
#
# print("isinstance check on slice1: ", isinstance(slice1, ge.dataset.PandasDataSet))
#
# print("isinstance check on slice2: ", isinstance(slice2, ge.dataset.PandasDataSet))
#
# print("isinstance check on slice3: ", isinstance(slice3, ge.dataset.PandasDataSet))
#
# print("isinstance check on slice4: ", isinstance(slice4, ge.dataset.PandasDataSet))
#
# print("isinstance check on slice5: ", isinstance(slice5, ge.dataset.PandasDataSet))
#
# print("isinstance check on slice6: ", isinstance(slice6, ge.dataset.PandasDataSet))
#
# print("isinstance check on slice7: ", isinstance(slice7, ge.dataset.PandasDataSet))
#
# print(df.find_expectations())
#
# print("-------------------------------------------------------------------")
#

print(slice1)

print(slice1.find_expectations())

print(df.discard_subset_failing_expectations)
print(slice1.discard_subset_failing_expectations)

df.discard_subset_failing_expectations = True

slice1 = df[['A', 'D']]

print(slice1.find_expectations())

print(df.discard_subset_failing_expectations)
print(slice1.discard_subset_failing_expectations)



# slice1.discard_failing_expectations()
#
# print(slice1.find_expectations())
#
# print("-------------------------------------------------------------------")
#
# print(slice2)
#
# print(slice2.find_expectations())
#
# print("-------------------------------------------------------------------")
#
# print(slice3)
#
# print(slice3.find_expectations())
#
# print("-------------------------------------------------------------------")
#
# print(slice4)
#
# print(slice4.find_expectations())
#
# print("-------------------------------------------------------------------")
#
# print(slice5)
#
# print(slice5.find_expectations())
#
# print("-------------------------------------------------------------------")
#
# print(slice6)
#
# print(slice6.find_expectations())
#
# print("-------------------------------------------------------------------")
#
# print(slice7)
#
# print(slice7.find_expectations())
#
# print("-------------------------------------------------------------------")
#
# print(slice8)
#
# print(slice8.find_expectations())
#
