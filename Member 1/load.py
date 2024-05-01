from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc, text
import pandas as pd
from sqlalchemy.pool import NullPool

# PostgreSQL connection string
postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/Health_Condition"

# Define operation to load health dimension data into PostgreSQL
@op(ins={'start': In(None)}, out=Out(bool))
def load_health_dimension(start=True):
    # Get logger for logging messages
    logger = get_dagster_logger()
    # Read the CSV file containing the health dimension data
    health_condition_columns = pd.read_csv("staging/health.csv", sep="\t")

    try:
        # Create an engine for connecting to PostgreSQL database
        engine = create_engine(postgres_connection_string, poolclass=NullPool)
        
        with engine.connect() as connection:
            # Truncate the existing table to prepare for new data
            truncate_statement = text("TRUNCATE TABLE public.health_condition;")
            connection.execute(truncate_statement)

        # Insert data into the table
        rowcount = health_condition_columns.to_sql(
            name="health_condition",
            schema="public",
            con=engine,
            index=False,
            if_exists="append"
        )
        
        # Log the number of records loaded
        logger.info(f"{rowcount} records loaded")
        # Dispose the engine
        engine.dispose()
        # Return True if records were loaded successfully
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        # Log the error if loading fails
        logger.error(f"Error: {error}")
        # Return False indicating loading failure
        return False
