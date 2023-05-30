import random

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier

from cardinality_expectations.generators import (
    gen_cardinality_params,
    gen_data_series
)

m = 1000

print("Generating cardinality params...")
cardinality_params_list = [gen_cardinality_params() for i in range(m)]
cardinality_params_df = pd.DataFrame(cardinality_params_list)
cardinality_params_df

print("Adding a random n to cardinality_params_df")
# cardinality_params_df["n"] = 0
# cardinality_params_df.n = cardinality_params_df.n.map(lambda x: int(random.uniform(5, 100)**2))
# cardinality_params_df["n"] = 100
cardinality_params_df["n"] = np.random.lognormal(mean=4, sigma=1.5, size=m)+10
cardinality_params_df.n = cardinality_params_df.n.map(lambda x: int(x))

print(cardinality_params_df)

print("Generating data...")
data = {}
for i, row in cardinality_params_df.iterrows():
    if i % 10 == 0:
        print(i)
    data[f"index_{i}"] = gen_data_series(
        cardinality_params=cardinality_params_list[i],
        n=cardinality_params_df.loc[i].n
    )
    # data[f"index_{i}"] = [gen_data_element(cardinality_params_list[i], j) for j in range(10000)]

generated_data = pd.DataFrame(data)

print("Adding nunique column to cardinality_params_df")
cardinality_params_df["nunique"] = None
for i in range(m):
    cardinality_params_df["nunique"].loc[i] = generated_data[f"index_{i}"][:cardinality_params_df.loc[i].n].nunique() 

print("Adding total_to_unique_ratio column to cardinality_params_df")
cardinality_params_df["total_to_unique_ratio"] = cardinality_params_df.n / cardinality_params_df["nunique"]


print("Adding pct_values column to cardinality_params_df")
top_values = 5
for j in range(top_values):
    cardinality_params_df[f"pct_value_{j}__ln"] = None
pct_value_columns = [f"pct_value_{j}__ln" for j in range(top_values)]

for i in range(m):
    n = cardinality_params_df.loc[i].n
    value_counts = list(pd.Series(generated_data[f"index_{i}"][:n]).value_counts())

    for j in range(top_values):
        cardinality_params_df[f"pct_value_{j}__ln"].loc[i] = np.log(float(value_counts[j])/n) if len(value_counts)>j else -100

print("Adding logged versions of three columns to cardinality_params_df")
cardinality_params_df["total_to_unique_ratio__ln"] = cardinality_params_df.total_to_unique_ratio.apply(lambda x: np.log(x) if x > 0 else 0)
cardinality_params_df["nunique__ln"] = cardinality_params_df["nunique"].apply(lambda x: np.log(x) if x > 0 else 0)
cardinality_params_df["n__ln"] = cardinality_params_df.n.apply(lambda x: np.log(x) if x > 0 else 0)

print(cardinality_params_df)

print("Creating X and y")
X = cardinality_params_df[["total_to_unique_ratio__ln", "nunique__ln", "n__ln"]+pct_value_columns]
y = cardinality_params_df.is_finite.map(int)

training_fraction = 0.8
training_cutoff = int(training_fraction*m)

print("Training Depth 1 model...")
# depth_1_model = LogisticRegression()
depth_1_model = MLPClassifier(hidden_layer_sizes=(5, 5), max_iter=1000)
depth_1_model.fit(X[:training_cutoff], y[:training_cutoff])

print("Depth 1 model results:")
print(depth_1_model.score(X[training_cutoff:], y[training_cutoff:]))
# print(depth_1_model.coef_)
# print(depth_1_model.intercept_)
# print(depth_1_model.predict_proba(X[training_cutoff:]))
# print(depth_1_model.predict(X[training_cutoff:]))
# print(y[training_cutoff:])

print(pd.crosstab(
    pd.Series(depth_1_model.predict(X[training_cutoff:])),
    pd.Series(list(y[training_cutoff:])),
))


print("Creating X and y")
X = cardinality_params_df[["total_to_unique_ratio__ln", "nunique__ln", "n__ln"]+pct_value_columns]
y = cardinality_params_df.category

training_fraction = 0.8
training_cutoff = int(training_fraction*m)

print("Training Depth 2 model...")
# depth_1_model = LogisticRegression()
depth_2_model = MLPClassifier(hidden_layer_sizes=(5, 5), max_iter=1000)
depth_2_model.fit(X[:training_cutoff], y[:training_cutoff])

print("Depth 1 model results:")
print(depth_2_model.score(X[training_cutoff:], y[training_cutoff:]))
# print(depth_2_model.coef_)
# print(depth_2_model.intercept_)
# print(depth_2_model.predict_proba(X[training_cutoff:]))
# print(depth_2_model.predict(X[training_cutoff:]))
# print(y[training_cutoff:])

crosstab = pd.crosstab(
    pd.Series(depth_2_model.predict(X[training_cutoff:])),
    pd.Series(list(y[training_cutoff:])),
)
crosstab = crosstab.reindex(["A_FEW","SEVERAL", "MANY", "DUPLICATED", "UNIQUE"])[["A_FEW","SEVERAL", "MANY", "DUPLICATED", "UNIQUE"]]
print(crosstab)


