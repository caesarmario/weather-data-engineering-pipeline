####
## ETL file for Compare Temperatures using Pandas and SQL
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import pandas as pd
import pandasql as ps

from utils.etl_helpers import ETLHelper
from utils.logging_helpers import get_logger
from utils.validation_helpers import ValidationHelper

logger = get_logger("etl_process")

class CompareTemperatures:
    """
    Class for performing Compare Temperatures processing.
    """
    def __init__(self):
        try:
            # Initialize helper and set folder paths
            self.helper             = ETLHelper()
            self.validation_helper  = ValidationHelper()
            self.folder_output      = "output"
            self.subfolder_raw      = "raw"
            self.subfolder_insights = "insights"
            self.load_process_dt    = self.helper.get_load_timestamp()
            self.config             = self.helper.load_config(self.subfolder_insights, "compare_temperatures")
            logger.info("Initialized CompareTemperatures class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to initialize CompareTemperatures class: {e}")
            raise

    def load_data(self):
        """
        Load the processed data from Parquet files based on the YAML configuration.
        """
        try:
            logger.info("-- Loading data from Parquet files")

            
            current_data = self.helper.read_parquet(self.folder_output, self.subfolder_raw, "current")
            
            forecast_data = self.helper.read_parquet(self.folder_output, self.subfolder_raw, "forecast")
            

            logger.info("Successfully loaded data from Parquet files.")
            return current_data, forecast_data, 
        
        except Exception as e:
            logger.error(f"!! Error loading Parquet files: {e}")
            raise

    def process_data(self):
        """
        Perform data transformation using SQL.
        """
        try:
            logger.info("-- Starting Compare Temperatures process")

            
            current_data = self.load_data()[0]
            
            forecast_data = self.load_data()[1]
            

            # SQL Transformation
            sql_query = """
            SELECT 
                c.location_id,
                c.date AS current_date,
                f.date AS forecast_date,
                c.temp_c AS current_temp_c,
                f.avgtemp_c AS forecast_temp_c,
                (c.temp_c - f.avgtemp_c) AS difference_temp_c,
                CASE 
                    WHEN c.temp_c > f.avgtemp_c THEN 'Higher'
                    WHEN c.temp_c < f.avgtemp_c THEN 'Lower'
                    ELSE 'Equal'
                END AS comparison,
                '{{ load_process_dt }}' AS load_process_dt
            FROM current_data c
            LEFT JOIN forecast_data f
            ON c.location_id = f.location_id
            """
            result_df = ps.sqldf(sql_query, locals())

            # Generate Output
            full_output_folder  = f"{self.folder_output}/{self.subfolder_insights}"
            output_file         = f"{full_output_folder}/xx_temperature_comparison_{self.load_process_dt}.csv"
            result_df.to_csv(output_file, index=False)
            logger.info(f"Compare Temperatures completed successfully and saved to {output_file}.")

        except Exception as e:
            logger.error(f"!! Error during Compare Temperatures process: {e}")
            raise

if __name__ == "__main__":
    try:
        logger.info("-- Starting Compare Temperatures script")
        processor = CompareTemperatures()
        processor.process_data()
        logger.info("-- Compare Temperatures script completed successfully")
    except Exception as e:
        logger.error(f"!! Critical error running the Compare Temperatures script: {e}")