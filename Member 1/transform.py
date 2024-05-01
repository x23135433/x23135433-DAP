from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
import os

# Define the structure of the transformed health condition data
TransformedHealthConditionDataFrame = create_dagster_pandas_dataframe_type(
name="TransformedHealthConditionDataFrame",
columns=[
    PandasColumn.string_column("measures"),
    PandasColumn.string_column("panel"),
    PandasColumn.integer_column("panel_num"),
    PandasColumn.string_column("unit"),
    PandasColumn.integer_column("unit_num"),
    PandasColumn.string_column("stub_name"),
    PandasColumn.integer_column("stub_name_num"),
    PandasColumn.string_column("stub_label"),
    PandasColumn.float_column("stub_label_num"),
    PandasColumn.string_column("years"),
    PandasColumn.integer_column("years_num"),
    PandasColumn.string_column("age"),
    PandasColumn.float_column("age_num"),
    PandasColumn.float_column("estimate"),
    PandasColumn.float_column("se"),
],


)

# Define operation to transform extracted health condition data
@op(ins={'start':In(None)},out=Out(TransformedHealthConditionDataFrame))
def transform_extracted_health(start = True) -> TransformedHealthConditionDataFrame:
    # Read the extracted health condition data from the staging directory
    health = pd.read_csv("staging/health.csv", sep="\t")
    # Ensure the data types are preserved
    health["measures"] = health["measures"]
    health["panel"] = health["panel"]
    health["panel_num"] = health["panel_num"]
    health["unit"] = health["unit"]
    health["unit_num"] = health["unit_num"]
    health["stub_name"] = health["stub_name"]
    health["stub_name_num"] = health["stub_name_num"]
    health["stub_label"] = health["stub_label"]
    health["stub_label_num"] = health["stub_label_num"]
    health["years"] = health["years"]
    health["years_num"] = health["years_num"]
    health["age"] = health["age"]
    health["age_num"] = health["age_num"]
    health["estimate"] = health["estimate"]
    health["se"] = health["se"]

    return health


# Define operation to stage transformed health condition data
@op(ins={'health': In(TransformedHealthConditionDataFrame)}, out=Out(None))
def stage_transformed_health(health):
    # Directory for staging the transformed data
    staging_dir = "staging"
    # Create the staging directory if it doesn't exist
    if not os.path.exists(staging_dir):
        os.makedirs(staging_dir)
    # Write the transformed health condition data to a CSV file in the staging directory
    health.to_csv(os.path.join(staging_dir, "health.csv"), index=False, sep="\t")
