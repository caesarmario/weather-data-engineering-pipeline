####
## ETL file for processing current weather data
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

# Processing data
class ProcessCurrent:
    def __init__(self):
        # Setting up the variables and config
        try:
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("raw", "current_config")
            self.load_dt           = self.helper.get_load_timestamp()
            self.folder_path       = "output"
            self.subfolder_path    = "raw"
            self.table_name        = "current"

            logger.info("Configuration for current table loaded successfully")
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self, data):
        # Main processing
        logger.info("- Starting the data processing for current table...")
        processed_data = []

        try:
            for key, data in data.items():
                try:
                    # Add batch ID, load timestamp, and city key
                    data = self.helper.add_remark_columns(data, self.batch_id, self.load_dt, key)

                    # Extract the columns based on the configuration
                    processed_record = self.helper.transform_helper(self.config, data, self.table_name, None)
                    processed_data.append(processed_record)
                    
                    logger.info(f"Successfully processed data for city: {key}")

                    processed_data.append(processed_record)
                    logger.info(f"Successfully processed data for city: {key}")
                except Exception as e:
                    # Log errors for individual city processing
                    logger.error(f"!! Error processing data for city: {key}, Error: {e}")
        except Exception as e:
            # Log unexpected errors during iteration
            logger.error(f"!! Unexpected error during data iteration: {e}")
            raise

        try:
            # Write to a CSV file
            date_csv    = self.helper.date_filename(processed_data[0]['last_updated'])
            file_path   = self.helper.write_csv(processed_data, self.folder_path, self.subfolder_path, self.table_name, date_csv)

            logger.info(f"Data successfully written to {file_path} !")
        except Exception as e:
            # Log errors during file writing
            logger.error(f"!! Error writing data to CSV: {e}")
            raise
