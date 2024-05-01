import webbrowser
from dagster import job
from extract import *
from transform import *
from load import *



@op(out=Out(bool))
def load_dimensions(health_dim):
    return health_dim



@job
def etl():
    health_dim=load_health_dimension(
        stage_transformed_health(
            transform_extracted_health(
                stage_extracted_health(
                    extract_health_condition_data()
                                            )
                                        )
                                    )
                                )
    
             
                            