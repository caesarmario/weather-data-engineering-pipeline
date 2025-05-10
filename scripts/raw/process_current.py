####
## ETL file for processing current data
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_utils import ETLHelper
from utils.logging_utils import logger
from utils.validation_utils import ValidationHelper

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
            self.date_str          = self.helper.date_filename(self.load_dt)
            self.bucket            = self.helper.staging_bucket
            self.object_name       = f"data/weather_data_{self.date_str}.json"
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
            data = self.helper.read_json(self.object_name)
        except Exception as e:
            logger.error(f"!! Unexpected error during reading JSON from bucket: {e}")
            raise

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
            object_name  = self.helper.upload_parquet_to_minio(processed_data, "current", self.bucket, date_parquet)

            logger.info(f">> Uploaded to MinIO bucket '{self.bucket}', object '{object_name}'")
        except Exception as e:
            logger.error(f"!! Error uploading Parquet to MinIO: {e}")
            raise