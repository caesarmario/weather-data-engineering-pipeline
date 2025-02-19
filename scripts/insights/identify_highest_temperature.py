####
## ETL file for identifying the day with the highest temperature for each city
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_helpers import get_logger
from utils.validation_helpers import ValidationHelper

import pandas as pd

logger = get_logger("etl_process")

class IdentifyHighestTemperature:
    """
    Class for identifying the day with the highest temperature for each city.
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
            self.config             = self.helper.load_config(self.subfolder_insights, "identify_highest_temp")
            logger.info("Initialized IdentifyHighestTemperature class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to initialize IdentifyHighestTemperature class: {e}")
            raise

    def load_data(self):
        """
        Load the processed CSV file for forecast data as a Pandas DataFrame.
        """
        try:
            logger.info("-- Loading forecast data from CSV file")
            forecast_data = self.helper.read_csv(self.folder_output, self.subfolder_raw, "forecast")
            logger.info("Successfully loaded forecast CSV file.")
            return forecast_data
        except Exception as e:
            logger.error(f"!! Error loading CSV file: {e}")
            raise

    def identify_highest_temperature(self):
        """
        Identify the day with the highest temperature for each city using Pandas.
        """
        try:
            logger.info("-- Starting highest temperature identification process")
            forecast_data = self.load_data()

            # Ensure temperature values are treated as float for comparison
            forecast_data["maxtemp_c"] = pd.to_numeric(forecast_data["maxtemp_c"], errors='coerce')

            # Identify the day with the highest temperature for each city
            highest_temperature = (
                forecast_data.loc[forecast_data.groupby("location_id")["maxtemp_c"].idxmax()]
                [["location_id", "date", "maxtemp_c"]]
            )

            # Add process date
            highest_temperature["load_process_dt"] = self.load_process_dt

            for column, column_config in self.config.items():
                # Convert data type using the helper function
                try:
                    if column in highest_temperature.columns:
                        highest_temperature[column] = highest_temperature[column].apply(
                            lambda value: self.helper.convert_data_type(value, column_config.get("data_type"))
                        )
                except Exception as e:
                    logger.error(f"!! Data type conversion error for column '{column}': {e}")
                
                # Perform validation if specified in config
                try:
                    if column_config.get('validation'):
                        validation_function = getattr(self.validation_helper, column_config['validation'], None)
                        if validation_function:
                            highest_temperature[column] = highest_temperature[column].apply(validation_function)  # Apply validation
                except Exception as e:
                    logger.error(f"!! Validation error for {column} in comparison result: {e}")

            # Generate the output file name using the latest processing date
            process_date = highest_temperature.iloc[0]["date"]

            # Generate Output
            full_output_folder  = f"{self.folder_output}/{self.subfolder_insights}"
            output_file         = f"{full_output_folder}/highest_temperature_by_city_{process_date}.csv"
            highest_temperature.to_csv(output_file, index=False)
            logger.info(f"Highest temperature identification completed successfully and saved to {output_file}.")

        except Exception as e:
            logger.error(f"!! Error during highest temperature identification process: {e}")
            raise

if __name__ == "__main__":
    try:
        logger.info("-- Starting highest temperature identification script")
        identifier = IdentifyHighestTemperature()
        identifier.identify_highest_temperature()
        logger.info("-- Highest temperature identification script completed successfully")
    except Exception as e:
        logger.error(f"!! Critical error running the highest temperature identification script: {e}")
