####
## ETL file for processing current data
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_utils import ETLHelper
from utils.logging_utils import get_logger
from utils.validation_utils import ValidationHelper

logger = get_logger("etl_process")

# Processing data
class ProcessCurrent:
    def __init__(self):
        try:
            # Setting up variables and config
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("raw", "current_config")
            self.load_dt           = self.helper.get_load_timestamp()
            self.folder_path       = "output"
            self.subfolder_path    = "raw"
            self.table_name        = "current"

            logger.info("Initialized current table class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self, data):
        # Main processing function
        logger.info(">> Starting the data processing for current table...")
        processed_data = []

        try:
            for key, record in data.items():
                try:
                    # Add batch ID, load timestamp, and city key
                    record = self.helper.add_remark_columns(record, self.batch_id, self.load_dt, key)

                    
                    # Extract columns based on the configuration
                    processed_record = self.helper.transform_helper(self.config, record, self.table_name, None)
                    processed_data.append(processed_record)

                    logger.info(f">> Successfully processed data for city: {key}")
                    

                except Exception as e:
                    logger.error(f"!! Error processing data for city: {key}, Error: {e}")
        except Exception as e:
            logger.error(f"!! Unexpected error during data iteration: {e}")
            raise

        try:
            # Write to a Parquet file
            date_parquet = self.helper.date_filename(processed_data[0]["last_updated"])
            file_path    = self.helper.write_parquet(processed_data, self.folder_path, self.subfolder_path, self.table_name, date_parquet)

            logger.info(f">> Data successfully written to {file_path}")
        except Exception as e:
            logger.error(f"!! Error writing data to Parquet: {e}")
            raise