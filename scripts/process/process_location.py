####
## ETL file for processing location data into Parquet
## Mario Caesar // caesarmario87@gmail.com
####

# --- Imports for ETL operations and logging
from utils.etl_utils import ETLHelper
from utils.logging_utils import logger

# --- Processor class: encapsulates setup and execution
class ProcessLocation:
    def __init__(self, exec_date=None):
        try:
            # Initialize helpers and load configuration
            self.helper            = ETLHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("process", "location_config")
            self.load_dt           = self.helper.get_load_timestamp()

            # Define source JSON path and target table
            self.bucket            = self.helper.staging_bucket
            self.object_name       = f"data/weather_data_{exec_date}.json"
            self.table_name        = "location"

            logger.info("Initialized location table class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self):
        # Main ETL routine: read, transform, and collect records
        logger.info(f">> Starting the data processing for location table - {self.object_name}...")
        processed_data = []

        # Read JSON input
        try:
            data = self.helper.read_json(self.object_name)
        except Exception as e:
            logger.error(f"!! Unexpected error during reading JSON from bucket: {e}")
            raise

        # Transform each record
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
            date_parquet = self.helper.date_filename(processed_data[0]["localtime"])
            object_name  = self.helper.upload_parquet_to_minio(processed_data, "location", self.bucket, date_parquet)

            logger.info(f">> Uploaded to MinIO bucket '{self.bucket}', object '{object_name}'")
        except Exception as e:
            logger.error(f"!! Error uploading Parquet to MinIO: {e}")
            raise

def run(exec_date, **kwargs):
    """
    Airflow entry-point for PythonOperator.
    """
    try:
        processor = ProcessLocation(exec_date)
        processor.process()
    except Exception as e:
        logger.error(f"!! location processing failed: {e}")
        raise

if __name__ == "__main__":
    run()