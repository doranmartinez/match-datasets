"""
Brute Force
- Calculate Matching accuracy
    - Iterate every row in dataset A:
        - iterate every row in dataset B:
            -For each col in row of dataset_A:
                -For each col in row of dataset_B:
                    - calculate A -> B accuracy score
                    - update/store max val and which row from B in column of dataset_A of that row

- Match items in dataset_A with items in dataset_B
    - Iterate every row in dataset A:
        - Iterate every row in dataset B:
            - calculate best_match_a_to_b by comparing accuracy scores
            - store in column of dataset_A

- Repeat for dataset_B

- Produce a final dataset merging the 2 datasets
    - Contains the following columns:
        "merged_id(property_id_zpid)",
        "property_id",
        "zpid",
        "best_candidate_a_to_b_zpid",
        "best_candidate_b_to_a_property_id",
        "accuracy_score",
        "street_address",
        "city",
        "zipcode",
        "state",
        "latitude",
        "longitude",
        "beds",
        "baths",
        "living_area",
        "lot_area",
"""

"""Merge Property Data"""
from fuzzywuzzy import fuzz
import pandas as pd


def calculate_accuracy(value_a, value_b, col_name):
    """Determine datatype and appropriate calculation"""

    numerical_set = {
        "beds",
        "baths",
        "living_area",
        "lot_area",
        "latitude",
        "longitude",
    }
    textual_set = {"street_address", "city", "state", "zipcode"}

    if pd.isnull(value_a) or pd.isnull(value_b):
        return 0

    if col_name in numerical_set:
        return 1 - abs(value_a - value_b) / max(value_a, value_b)

    if col_name in textual_set:
        return fuzz.ratio(str(value_a), str(value_b)) / 100.0

    return 0


def matching_accuracy(df_a, df_b):
    """
    Per Row Accuracy Calculation
    """
    columns_to_exclude = {"best_matching_row_b", "max_accuracy"}
    df_a["best_matching_row_b"] = None  # TODO: f"best_matching_row_{b}"
    df_a["max_accuracy"] = 0

    for index_a, row_a in df_a.iterrows():
        max_accuracy = 0
        best_match_row_b = None

        for index_b, row_b in df_b.iterrows():
            accuracy = 0

            for col in df_a.columns:
                if col not in columns_to_exclude:
                    accuracy += calculate_accuracy(row_a[col], row_b[col], col)

            if accuracy > max_accuracy:  # The first max_accuracy is stored
                max_accuracy = accuracy
                best_match_row_b = index_b

        df_a.at[index_a, "best_matching_row_b"] = best_match_row_b
        df_a.at[index_a, "max_accuracy"] = max_accuracy


# Might want to calculate total accuracy percentage
# Sum up 'max_accuracy' values
# # Calculate Matching Accuracy Percentage
# matching_accuracy = (len(df_a) / len(df_b)) * 100


def main():
    """
    Main Function
    """
    df_a = pd.read_csv("A.csv", delimiter=";")
    df_b = pd.read_csv("B.csv", delimiter=";")
    df_a.set_index("property_id", inplace=True)
    df_b.set_index("zpid", inplace=True)

    matching_accuracy(df_a, df_b)


if __name__ == "__main__":
    main()
