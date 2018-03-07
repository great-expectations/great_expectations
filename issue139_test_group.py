import great_expectations as ge

df = ge.dataset.PandasDataSet({
    'A' : ['a', 'b', 'a', 'b', 'c', 'a'],
    'B' : [ 1,   2,   3,   3,   2,   1],
    'C' : ['y', 'y', 'n', 'y', 'y', 'n']
})

print(df)

grouped_rows = df.groupby('A', axis=0)
grouped_cols = df.groupby(df.dtypes, axis=1)

print("----------------------------list grouped_rows----------------------------")
print(list(grouped_rows))

print("----------------------------list grouped_cols----------------------------")
print(list(grouped_cols))

print("----------------------------describe grouped_rows----------------------------")
print(grouped_rows.describe())

print("----------------------------describe grouped_cols----------------------------")
print(grouped_cols.describe())

print("----------------------------iterate grouped_rows----------------------------")
for name,group in grouped_rows:
    print(name)
    print(group)

print("----------------------------iterate grouped_cols----------------------------")
for name,group in grouped_cols:
    print(name)
    print(group)

print("----------------------------grouped_rows groups----------------------------")
print(grouped_rows.groups)

print("----------------------------grouped_cols groups----------------------------")
print(grouped_cols.groups)

print("----------------------------grouped_rows count----------------------------")
print(grouped_rows.count())

print("----------------------------grouped_cols count----------------------------")
print(grouped_cols.count())

print("----------------------------grouped_rows sum----------------------------")
print(grouped_rows.sum())

print("----------------------------grouped_cols sum----------------------------")
print(grouped_cols.sum())
