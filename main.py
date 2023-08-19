"""Match Datasets"""
import logging

import pandas as pd
from fuzzywuzzy import fuzz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("logger")


def calculate_accuracy(value_x, value_y, column_name):
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

    if pd.isnull(value_x) or pd.isnull(value_y):
        return 0

    if column_name in numerical_set:
        max_value = max(value_x, value_y)

        if not max_value:  # Avoids Division by Zero
            return 0

        return 1 - abs(value_x - value_y) / max_value

    if column_name in textual_set:
        return fuzz.ratio(str(value_x), str(value_y)) / 100.0

    return 0


def matching_accuracy(row, column_names):
    """Match Accuracy for row from df_a across columns from df_b"""
    accuracy = 0
    column_mapping = {
        name: name.replace("_x", "_y") for name in column_names if "_x" in name
    }
    num_columns = len(column_mapping)  # To normalize accuracy values

    for value_x, value_y in column_mapping.items():
        col_name = value_x.replace("_x", "")
        accuracy += (
            calculate_accuracy(getattr(row, value_x), getattr(row, value_y), col_name)
            / num_columns
        )
    return accuracy


def main():
    """
    Main Function
    """
    logger.info("Importing dataset A...")
    df_a = pd.read_csv("A.csv", delimiter=";")

    logger.info("importing dataset B...")
    df_b = pd.read_csv("B.csv", delimiter=";")

    logger.info("Performing left join...")
    left = pd.merge(
        df_a,
        df_b,
        left_on="street_address",
        right_on="street_address",
        how="left",
        suffixes=("_x", "_y"),
    )

    logger.info("Performing right join...")
    right = pd.merge(
        df_a,
        df_b,
        left_on="street_address",
        right_on="street_address",
        how="right",
        suffixes=("_x", "_y"),
    )

    logger.info("Calculating match accuracy of Left joined rows...")
    left["match_accuracy"] = left.apply(
        lambda row: matching_accuracy(row, left.columns.tolist()), axis=1
    )

    # Drop non-NaN values from right
    logger.info("Dropping rows with non-NaN values from right DataFrame...")
    right["match_accuracy"] = 0
    right_cleaned = right[~right["property_id"].notna()]

    # Concat left and right datasets
    logger.info("Concatenating and removing duplicates...")
    merged = pd.concat([left, right_cleaned], ignore_index=True)
    final_dataset = merged.drop_duplicates()

    # Export 'merged_unique' DataFrame to a CSV file
    output_filename = "final_dataset.csv"
    logger.info(
        "Exporting 'merged_unique' DataFrame to '{}'...".format(output_filename)
    )
    final_dataset.to_csv(output_filename, sep=";", index=False)
    # Reading in final_dataset.csv produces the following warning:
    # DtypeWarning: Columns (2,4) have mixed types. Specify dtype option on import or set low_memory=False.


if __name__ == "__main__":
    main()
