####
## ETL file for comparing current and forecasted temperatures using Pandas
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

import pandas as pd

class CompareTemperatures:
    """
    Class for comparing current temperature with forecasted temperatures for each city.
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
        Load the processed CSV files for current and forecast data as Pandas DataFrames.
        """
        try:
            logger.info("-- Loading data from CSV files")
            current_data  = self.helper.read_csv(self.folder_output, self.subfolder_raw, "current")
            forecast_data = self.helper.read_csv(self.folder_output, self.subfolder_raw, "forecast")
            logger.info("Successfully loaded current and forecast CSV files.")

            return current_data, forecast_data
        
        except Exception as e:
            logger.error(f"!! Error loading CSV files: {e}")
            raise

    def compare_temperatures(self):
        """
        Compare current temperatures with forecasted temperatures using Pandas.
        """
        try:
            logger.info("-- Starting temperature comparison process")
            current_data, forecast_data = self.load_data()
            comparisons                 = []

            # Loop through current data and compare with forecast
            for index, current_row in current_data.iterrows():
                location_id   = current_row["location_id"]
                current_temp  = float(current_row["temp_c"])
                current_date  = current_row["date"]

                # Filter forecast data for the same location
                city_forecast = forecast_data[forecast_data["location_id"] == location_id]

                if city_forecast.empty:
                    logger.warning(f"No forecast data found for city: {location_id}")

                for idx, forecast_row in city_forecast.iterrows():
                    forecast_temp     = float(forecast_row["avgtemp_c"])
                    forecast_date     = forecast_row["date"]
                    difference_temp_c = current_temp - forecast_temp
                    comparison        = "Higher" if current_temp > forecast_temp else "Lower" if current_temp < forecast_temp else "Equal"

                    # Perform the temperature comparison
                    comparison_result = {
                        "location_id"       : location_id,
                        "current_date"      : current_date,
                        "forecast_date"     : forecast_date,
                        "current_temp_c"    : current_temp,
                        "forecast_temp_c"   : forecast_temp,
                        "difference_temp_c" : difference_temp_c,
                        "comparison"        : comparison,
                        "load_process_dt"   : self.load_process_dt
                    }
                    comparisons.append(comparison_result)

                for column, column_config in self.config.items():
                    # Convert data type using the helper function
                    try:
                        field_value = comparison_result[column]
                        field_value = self.helper.convert_data_type(field_value, column_config.get("data_type"))
                        comparison_result[column] = field_value
                    except Exception as e:
                        logger.error(f"!! Data type conversion error: {e}")

                    # Perform validation if specified in config
                    try:
                        if column_config.get('validation'):
                            validation_function = getattr(self.validation_helper, column_config['validation'], None)
                            if validation_function:
                                field_value = comparison_result[column]  # Fetch the field value for validation
                                comparison_result[column] = validation_function(field_value)  # Apply validation
                    except Exception as e:
                        logger.error(f"!! Validation error for {column} in comparison result: {e}")

            # Convert the results to a DataFrame and save
            comparisons_df = pd.DataFrame(comparisons)

            # Generate Output
            full_output_folder  = f"{self.folder_output}/{self.subfolder_insights}"
            output_file         = f"{full_output_folder}/temperature_comparison_{current_date}.csv"
            comparisons_df.to_csv(output_file, index=False)
            logger.info(f"Temperature comparison completed successfully and saved to {output_file}.")

        except Exception as e:
            logger.error(f"!! Error during temperature comparison process: {e}")
            raise

if __name__ == "__main__":
    try:
        logger.info("-- Starting temperature comparison script")
        comparator = CompareTemperatures()
        comparator.compare_temperatures()
        logger.info("-- Temperature comparison script completed successfully")
    except Exception as e:
        logger.error(f"!! Critical error running the temperature comparison script: {e}")
