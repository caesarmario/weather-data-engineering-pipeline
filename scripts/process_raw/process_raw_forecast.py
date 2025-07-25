####
## ETL file for processing raw forecast data into Parquet
## Mario Caesar // caesarmario87@gmail.com
####

# --- Imports for ETL operations and logging
from utils.etl_utils import ETLHelper
from utils.logging_utils import logger

import argparse
import json
import sys

# --- Processor class: encapsulates setup and execution
class ProcessForecast:
    def __init__(self, exec_date, credentials: dict):
        try:
            # Initialize helpers and load configuration
            self.helper            = ETLHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("l0_weather", "forecast_config")
            self.load_dt           = self.helper.get_load_timestamp()

            # Define source JSON path and target table
            self.object_name       = f"data/weather_data_{exec_date}.json"
            self.table_name        = "forecast"
            self.credentials       = credentials
            self.bucket_raw        = credentials.get("MINIO_BUCKET_RAW")
            self.bucket_staging    = credentials.get("MINIO_BUCKET_STAGING")

            logger.info("Initialized forecast table class successfully.")
        except Exception as e:
            raise RuntimeError(f"!! Failed to load configuration: {e}")

    def process(self):
        # Main ETL routine: read, transform, and collect records
        logger.info(f">> Starting the data processing for raw forecast table - {self.object_name}...")
        processed_data = []

        # Read JSON input
        try:
            data = self.helper.read_json(self.bucket_raw, self.object_name, self.credentials)
        except Exception as e:
            raise RuntimeError(f"!! Unexpected error during reading JSON from bucket: {e}")

        # Transform each record
        try:
            for key, record in data.items():
                try:
                    # Add batch ID, load timestamp, and city key
                    record = self.helper.add_remark_columns(record, self.batch_id, self.load_dt, key)

                    # Extract forecast data correctly
                    forecast_days = record.get('forecast', {}).get('forecastday', [])
                    for day in forecast_days:
                        processed_record = self.helper.transform_helper(self.config, record, self.table_name, day)
                        processed_data.append(processed_record)

                        logger.info(f">> Successfully processed forecast data for city: {key} on {day.get('date')}")

                except Exception as e:
                    raise RuntimeError(f"!! Error processing data for city: {key}, Error: {e}")
        except Exception as e:
            raise RuntimeError(f"!! Unexpected error during data iteration: {e}")

        try:
            # Write to a Parquet file
            date_parquet = self.helper.date_filename(processed_data[0]["date"])
            object_name  = self.helper.upload_parquet_to_minio(processed_data, "forecast", self.bucket_staging, date_parquet, self.credentials)

            logger.info(f">> Uploaded to MinIO bucket '{self.bucket_staging}', object '{object_name}'")
        except Exception as e:
            raise RuntimeError(f"!! Error uploading Parquet to MinIO: {e}")


def main():
    # Retrieving arguments
    try:
        parser = argparse.ArgumentParser(description="Process 'forecast' JSON → Parquet")
        parser.add_argument("--exec_date", type=str, required=True, help="Execution date in YYYY-MM-DD format")
        parser.add_argument("--credentials", type=str, required=True, help="MinIO credentials")
        args = parser.parse_args()
    except Exception as e:
        raise RuntimeError(f"!! One of the arguments is empty! - {e}")

    # Preparing variables & creds
    try:
        creds         = json.loads(args.credentials)
    except Exception as e:
        raise RuntimeError(f"!! Failed to parse JSON credentials: {e}")

    # Running processor
    try:
        processor = ProcessForecast(args.exec_date, creds)
        processor.process()
    except Exception as e:
        raise RuntimeError(f"!! Error running processor - {e}")

if __name__ == "__main__":
    main()