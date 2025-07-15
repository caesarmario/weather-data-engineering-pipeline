####
## ETL file for processing location data into Parquet
## Mario Caesar // caesarmario87@gmail.com
####

# --- Imports for ETL operations and logging
from utils.etl_utils import ETLHelper
from utils.logging_utils import logger

import argparse
import json

# --- Processor class: encapsulates setup and execution
class ProcessLocation:
    def __init__(self, exec_date, credentials: dict):
        try:
            # Initialize helpers and load configuration
            self.helper            = ETLHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("l0_weather", "location_config")
            self.load_dt           = self.helper.get_load_timestamp()

            # Define source JSON path and target table
            self.object_name       = f"data/weather_data_{exec_date}.json"
            self.table_name        = "location"
            self.credentials       = credentials
            self.bucket_raw        = credentials.get("MINIO_BUCKET_RAW")
            self.bucket_staging    = credentials.get("MINIO_BUCKET_STAGING")

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
            data = self.helper.read_json(self.bucket_raw, self.object_name, self.credentials)
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
            object_name  = self.helper.upload_parquet_to_minio(processed_data, "location", self.bucket_staging, date_parquet, self.credentials)

            logger.info(f">> Uploaded to MinIO bucket '{self.bucket_staging}', object '{object_name}'")
        except Exception as e:
            logger.error(f"!! Error uploading Parquet to MinIO: {e}")
            raise


def main():
    # Retrieving arguments
    try:
        parser = argparse.ArgumentParser(description="Process 'location' JSON → Parquet")
        parser.add_argument("--exec_date", type=str, required=True, help="Execution date in YYYY-MM-DD format")
        parser.add_argument("--credentials", type=str, required=True, help="MinIO credentials")
        args = parser.parse_args()
    except Exception as e:
        logger.error(f"!! One of the arguments is empty! - {e}")

    # Preparing variables & creds
    try:
        creds         = json.loads(args.credentials)
    except Exception as e:
        logger.error(f"!! Failed to parse JSON credentials: {e}")
        raise ValueError("!! Invalid credentials JSON format")

    # Running processor
    try:
        processor = ProcessLocation(args.exec_date, creds)
        processor.process()
    except Exception as e:
        logger.error(f"!! Error running processor - {e}")

if __name__ == "__main__":
    main()