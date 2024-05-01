from pymongo import MongoClient
from dagster import op, Out, In, DagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from datetime import datetime
import pandas as pd
import os

# MongoDB connection string
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"

# Create Dagster Pandas DataFrame type for health condition
HealthConditionDataFrame = create_dagster_pandas_dataframe_type(
    name="HealthConditionDataFrame",
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
# Dictionary to map column names to their respective names in the data frame
health_condition_columns = {
    "measures": "measures",
    "panel": "panel",
    "panel_num": "panel_num",
    "unit": "unit",
    "unit_num": "unit_num",
    "stub_name": "stub_name",
    "stub_name_num": "stub_name_num",
    "stub_label": "stub_label",
    "stub_label_num": "stub_label_num",
    "years": "years",
    "years_num": "years_num",
    "age": "age",
    "age_num": "age_num",
    "estimate": "estimate",
    "se": "se"
}

# Define operation to extract health condition data
@op(ins={'start': In(bool)}, out=Out(HealthConditionDataFrame))
def extract_health_condition_data(start = True) -> HealthConditionDataFrame:
    # Establish connection to MongoDB
    conn = MongoClient(mongo_connection_string)
    # Access the database
    db = conn["Health_Condition"]
    # Get health condition data from the collection
    health = pd.DataFrame(db.Health_Condition_1.find({}))
    # Drop the '_id' column as it's not needed
    health.drop(
        columns=["_id"],
        axis=1,
        inplace=True
    )
    
    
    conn.close()
    return health
# Define operation to stage extracted health condition data
@op(ins={'health': In(HealthConditionDataFrame)}, out=Out(None))
def stage_extracted_health(health):
    # Directory for staging the data
    staging_dir = "staging"
     # Create the staging directory if it doesn't exist
    if not os.path.exists(staging_dir):
        os.makedirs(staging_dir)
    # Write the health condition data to a CSV file in the staging directory
    health.to_csv(os.path.join(staging_dir, "health.csv"), index=False, sep="\t")

