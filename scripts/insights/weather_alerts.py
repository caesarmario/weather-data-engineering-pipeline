####
## ETL file for identifying and flagging weather alerts based on thresholds
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

import pandas as pd

class WeatherAlerts:
    """
    Class for identifying and flagging weather alerts based on predefined thresholds.
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
            self.config             = self.helper.load_config(self.subfolder_insights, "weather_alerts")
            logger.info("-- Initialized WeatherAlerts class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to initialize WeatherAlerts class: {e}")
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

    def identify_weather_alerts(self):
        """
        Identify and flag weather alerts based on predefined thresholds.
        """
        try:
            logger.info("-- Starting weather alerts identification process")
            forecast_data = self.load_data()

            # Convert temperature and precipitation columns to numeric
            forecast_data["maxtemp_c"]          = pd.to_numeric(forecast_data["maxtemp_c"], errors='coerce')
            forecast_data["totalprecip_mm"]     = pd.to_numeric(forecast_data["totalprecip_mm"], errors='coerce')
            forecast_data["maxwind_kph"]        = pd.to_numeric(forecast_data["maxwind_kph"], errors='coerce')

            # Define alert conditions
            forecast_data["alert_extreme_heat"] = forecast_data["maxtemp_c"] > 35
            forecast_data["alert_storm"]        = (forecast_data["totalprecip_mm"] > 20) | (forecast_data["maxwind_kph"] > 15)

            # Filter rows where any alert is triggered
            alert_data = forecast_data[
                forecast_data["alert_extreme_heat"] | forecast_data["alert_storm"]
            ][["location_id", "date", "maxtemp_c", "totalprecip_mm", "maxwind_kph", "alert_extreme_heat", "alert_storm"]]

            # Add the process date to the report
            alert_data["load_process_dt"]       = self.load_process_dt
            
            for column, column_config in self.config.items():
                # Convert data type using the helper function
                try:
                    if column in alert_data.columns:
                        alert_data[column] = alert_data[column].apply(
                            lambda value: self.helper.convert_data_type(value, column_config.get("data_type"))
                        )
                except Exception as e:
                    logger.error(f"!! Data type conversion error for column '{column}': {e}")

                # Perform validation if specified in config
                try:
                    if column_config.get('validation'):
                        validation_function = getattr(self.validation_helper, column_config['validation'], None)
                        if validation_function:
                            alert_data[column] = alert_data[column].apply(validation_function)  # Apply validation
                except Exception as e:
                    logger.error(f"!! Validation error for {column} in comparison result: {e}")

            # Generate the output file name using the latest processing date
            process_date = forecast_data["date"].min()
            
            # Generate Output
            full_output_folder  = f"{self.folder_output}/{self.subfolder_insights}"
            output_file         = f"{full_output_folder}/weather_alerts_summary_{process_date}.csv"
            alert_data.to_csv(output_file, index=False)
            logger.info(f"Weather alerts identified successfully and saved to {output_file}.")
        
        except Exception as e:
            logger.error(f"!! Error during weather alerts identification: {e}")
            raise

if __name__ == "__main__":
    try:
        logger.info("-- Starting weather alerts identification script")
        alert_identifier = WeatherAlerts()
        alert_identifier.identify_weather_alerts()
        logger.info("-- Weather alerts identification script completed successfully")
    except Exception as e:
        logger.error(f"!! Critical error running the weather alerts identification script: {e}")
