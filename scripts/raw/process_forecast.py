####
## ETL file for processing forecast weather data
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

# Processing data
class ProcessForecast:
    def __init__(self):
        # Setting up the variables and config
        try:
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("raw", "forecast_config")
            self.load_dt           = self.helper.get_load_timestamp()
            self.folder_path       = "output"
            self.subfolder_path    = "raw"
            self.table_name        = "forecast"

            logger.info("Configuration for forecast table loaded successfully")
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self, data):
        # Main processing
        logger.info("- Starting the data processing for forecast table...")
        processed_data = []

        try:
            for key, data in data.items():
                try:
                    # Add batch ID, load timestamp, and city key
                    data = self.helper.add_remark_columns(data, self.batch_id, self.load_dt, key)

                    # Extract forecast data correctly
                    forecast_days = data.get('forecast', {}).get('forecastday', [])
                    for day in forecast_days:
                        
                        # Extract the columns based on the configuration
                        processed_record = self.helper.transform_helper(self.config, data, self.table_name, day)
                        processed_data.append(processed_record)

                        logger.info(f"Successfully processed forecast data for city: {key} on {day.get('date')}")
                except Exception as e:
                    # Log errors for individual city processing
                    logger.error(f"!! Error processing forecast data for city: {key}, Error: {e}")
        except Exception as e:
            # Log unexpected errors during iteration
            logger.error(f"!! Unexpected error during data iteration: {e}")
            raise

        # Writing the data to CSV file
        try:
            # Write to a CSV file
            date_csv    = self.helper.date_filename(processed_data[0]['date'])
            file_path   = self.helper.write_csv(processed_data, self.folder_path, self.subfolder_path, self.table_name, date_csv)

            logger.info(f"Data successfully written to {file_path}")
        except Exception as e:
            # Log errors during file writing
            logger.error(f"!! Error writing data to CSV: {e}")
            raise