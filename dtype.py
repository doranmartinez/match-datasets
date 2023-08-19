"""
My attempt to get rid of the Dtype warning by find a better merging protocol.
I attempt to perform the accuracy computation on the inner joined data and then merge it with the outer join.
"""
import pandas as pd
from main import matching_accuracy

df_a = pd.read_csv("A.csv", delimiter=";")
df_b = pd.read_csv("B.csv", delimiter=";")

# Make sure dtype's match
for column in df_a.columns:
    if column in df_b.columns:
        dtype_a = df_a[column].dtype
        dtype_b = df_b[column].dtype
        if dtype_a != dtype_b:
            df_a[column] = df_a[column].astype(dtype_b)

inner = pd.merge(
    df_a,
    df_b,
    left_on="street_address",
    right_on="street_address",
    how="inner",
    suffixes=("_x", "_y"),
)

outer = pd.merge(
    df_a,
    df_b,
    left_on="street_address",
    right_on="street_address",
    how="outer",
    suffixes=("_x", "_y"),
)

outer["match_accuracy"] = 0.0
inner["match_accuracy"] = inner.apply(
    lambda row: matching_accuracy(row, inner.columns.tolist()), axis=1
)

merged = pd.concat([outer, inner], ignore_index=True)
final_dataset = merged.drop_duplicates()
final_dataset.to_csv("final_dataset.csv", sep=";", index=False)

# Reading in final_dataset.csv produces the following warning:
# DtypeWarning: Columns (2,4) have mixed types. Specify dtype option on import or set low_memory=False.
