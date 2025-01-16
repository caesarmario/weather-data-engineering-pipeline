####
## ETL file for calculating the min, max, and average temperatures for each city
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

import pandas as pd

class TemperatureStatistics:
    """
    Class for calculating the minimum, maximum, and average temperatures for each city.
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
            self.config             = self.helper.load_config(self.subfolder_insights, "temperature_stats")
            logger.info("-- Initialized TemperatureStatistics class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to initialize TemperatureStatistics class: {e}")
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

    def calculate_temperature_statistics(self):
        """
        Calculate the minimum, maximum, and average temperatures for each city.
        """
        try:
            logger.info("-- Starting temperature statistics calculation")
            forecast_data = self.load_data()

            # Convert temperature columns to numeric values
            temperature_columns                = ["maxtemp_c", "maxtemp_f", "mintemp_c", "mintemp_f", "avgtemp_c", "avgtemp_f"]
            forecast_data[temperature_columns] = forecast_data[temperature_columns].apply(pd.to_numeric, errors='coerce')

            # Group by location and calculate temperature statistics
            temperature_stats = (
                forecast_data
                .groupby("location_id")
                .agg(
                    min_temp_c       = ("mintemp_c", "min"),
                    max_temp_c       = ("maxtemp_c", "max"),
                    avg_temp_c       = ("avgtemp_c", "mean"),
                    min_temp_f       = ("mintemp_f", "min"),
                    max_temp_f       = ("maxtemp_f", "max"),
                    avg_temp_f       = ("avgtemp_f", "mean"),
                    date_range_start = ("date", "min"),
                    date_range_end   = ("date", "max")
                )
                .reset_index()
            )

            # Temperature variation calculation
            temperature_stats["temperature_variation_c"] = (temperature_stats["max_temp_c"] - temperature_stats["min_temp_c"])
            temperature_stats["temperature_variation_f"] = (temperature_stats["max_temp_f"] - temperature_stats["min_temp_f"])

            # Add process date and total days covered
            temperature_stats["total_days"]      = (pd.to_datetime(temperature_stats["date_range_end"]) - pd.to_datetime(temperature_stats["date_range_start"])).dt.days + 1
            temperature_stats["load_process_dt"] = self.load_process_dt

            for column, column_config in self.config.items():
                # Convert data type using the helper function
                try:
                    if column in temperature_stats.columns:
                        temperature_stats[column] = temperature_stats[column].apply(
                            lambda value: self.helper.convert_data_type(value, column_config.get("data_type"))
                        )
                except Exception as e:
                    logger.error(f"!! Data type conversion error for column '{column}': {e}")

                # Perform validation if specified in config
                try:
                    if column_config.get('validation'):
                        validation_function = getattr(self.validation_helper, column_config['validation'], None)
                        if validation_function:
                            temperature_stats[column] = temperature_stats[column].apply(validation_function)  # Apply validation
                except Exception as e:
                    logger.error(f"!! Validation error for {column} in comparison result: {e}")
           
            # Generate the output file name using the latest processing date
            process_date = forecast_data["date"].min()

            # Generate Output
            full_output_folder  = f"{self.folder_output}/{self.subfolder_insights}"
            output_file         = f"{full_output_folder}/temperature_statistics_by_city_{process_date}.csv"
            temperature_stats.to_csv(output_file, index=False)
            logger.info(f"Temperature statistics calculated successfully and saved to {output_file}.")

        except Exception as e:
            logger.error(f"!! Error during temperature statistics calculation: {e}")
            raise

if __name__ == "__main__":
    try:
        logger.info("-- Starting temperature statistics calculation script")
        stats_calculator = TemperatureStatistics()
        stats_calculator.calculate_temperature_statistics()
        logger.info("-- Temperature statistics calculation script completed successfully")
    except Exception as e:
        logger.error(f"!! Critical error running the temperature statistics calculation script: {e}")
