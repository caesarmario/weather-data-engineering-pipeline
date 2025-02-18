####
## ETL file for processing location data
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

# Processing data
class ProcessLocation:
    def __init__(self):
        try:
            # Setting up variables and config
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("raw", "location_config")
            self.load_dt           = self.helper.get_load_timestamp()
            self.folder_path       = "output"
            self.subfolder_path    = "raw"
            self.table_name        = "location"

            logger.info("Initialized location table class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self, data):
        # Main processing function
        logger.info(">> Starting the data processing for location table...")
        processed_data = []

        try:
            for key, data in data.items():
                try:
                    # Add batch ID, load timestamp, and city key
                    data = self.helper.add_remark_columns(data, self.batch_id, self.load_dt, key)

                    # Extract the columns based on the configuration
                    processed_record = self.helper.transform_helper(self.config, data, self.table_name, None)
                    processed_data.append(processed_record)
                    
                    logger.info(f">> Successfully processed data for city: {key}")
                except Exception as e:
                    # Log errors for individual city processing
                    logger.error(f"!! Error processing data for city: {key}, Error: {e}")
        except Exception as e:
            # Log unexpected errors
            logger.error(f"!! Unexpected error during data iteration: {e}")
            raise
        
        try:
            # Write to a CSV file
            date_csv    = self.helper.date_filename(processed_data[0]['localtime'])
            file_path   = self.helper.write_parquet(processed_data, self.folder_path, self.subfolder_path, self.table_name, date_csv)

            logger.info(f">> Data successfully written to {file_path} !")
        except Exception as e:
            # Log errors during file writing
            logger.error(f"!! Error writing data to CSV: {e}")
            raise