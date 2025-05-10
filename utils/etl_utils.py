####
## Helper file for processing data 
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv
from io import BytesIO

import json
import uuid
import os
import pandas as pd

from utils.logging_utils import logger
from utils.validation_utils import ValidationHelper

class ETLHelper:

    def __init__(self):

        # Load environment variables
        load_dotenv()

        # Creds
        ## MinIO
        self.endpoint       = os.getenv("MINIO_ENDPOINT")
        self.access         = os.getenv("MINIO_ACCESS_KEY")
        self.secret         = os.getenv("MINIO_SECRET_KEY")
        self.raw_bucket     = os.getenv("MINIO_BUCKET_RAW")
        self.staging_bucket = os.getenv("MINIO_BUCKET_STAGING")
        self.curated_bucket = os.getenv("MINIO_BUCKET_CURATED")
        self.minio_client   = self.create_minio_conn()
    

    # Functions
    def generate_batch_id(self):
        """
        Generate a unique batch identifier using UUID.

        Returns:
            str: A unique identifier for the ETL batch run.
        """
        return str(uuid.uuid4())


    def get_load_timestamp(self):
        """
        Generate the current timestamp in a readable format.

        Returns:
            str: Current timestamp in 'YYYY-MM-DD HH:mm:ss' format.
        """
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def create_minio_conn(self):
        """
        Function to create a MinIO client.
        
        Returns:
            MinIO client connection.
        """
        return Minio(
            self.endpoint,
            access_key=self.access,
            secret_key=self.secret,
            secure=False
        )


    def read_json(self, object_name: str):
        """
        Read JSON from MinIO, parse, dan return dict.

        Args:
            object_name (str): Object name in MinIO to be read as JSON

        Returns:
            dict: parsed JSON data
        """
        try:
            resp = self.minio_client.get_object(self.raw_bucket, object_name)
            data = json.loads(resp.read().decode("utf-8"))
            return data

        except Exception as e:
            logger.error(f"!! Failed to read JSON from MinIO bucket '{self.raw_bucket}', object '{object_name}': {e}")
            raise

        finally:
            resp.close()
            resp.release_conn()


    def load_config(self, subfolder, config_name):
        """
        Load configuration from a JSON file.

        Args:
            config_name (str): The name of configuration file.

        Returns:
            dict: Configuration data loaded from the file.
        """

        with open(f'schema_config/{subfolder}/{config_name}.json', 'r') as file:
            return json.load(file)


    def upload_parquet_to_minio(self, data, context, bucket, date_parquet):
        """
        Convert list-of-dict to Parquet in-memory, upload to MinIO staging bucket.

        Args:
            data (list[dict]): hasil transformasi.
            context (str): nama table (dipakai di object key).
            date_parquet (str): YYYY-MM-DD untuk nama file.

        Returns:
            str: object_name yang diupload.
        """
        try:
            # Convert to Dataframe, then Dataframe to Parquet bytes
            df     = pd.DataFrame(data)
            buffer = BytesIO()

            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            # Build object path (e.g. staging/forecast/2025/05/10/forecast_2025-05-10.parquet)
            year, month, day = date_parquet.split("-")
            object_name      = f"{context}/{year}/{month}/{day}/{context}_{date_parquet}.parquet"

            # Upload
            self.minio_client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            return object_name
        except Exception as e:
            logger.error(f"!! Failed to upload Parquet file to MinIO bucket '{self.raw_bucket}', object '{object_name}': {e}")
            raise


    def convert_data_type(self, value, data_type):
        """
        Convert the value to the specified data type.

        Args:
            value: The value to be converted.
            data_type (str): The target data type (STRING, FLOAT, INTEGER, BOOLEAN).

        Returns:
            Converted value or None if conversion fails.
        """
        try:
            if data_type == 'STRING':
                return str(value)
            elif data_type == 'FLOAT':
                return float(value)
            elif data_type == 'INTEGER':
                return int(value)
            elif data_type == 'BOOLEAN':
                return bool(int(value)) if str(value).isdigit() else bool(value)
            elif data_type == 'DATE':
                return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")                   # Convert to YYYY-MM-DD
            elif data_type == 'DATETIME':
                return datetime.strptime(value, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M")       # Convert to YYYY-MM-DD HH:MM
            elif data_type == 'TIMESTAMP':
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S") # Convert to YYYY-MM-DD HH:MM:SS
            else:
                logger.warning(f"Unknown data type specified: {data_type}. Returning value as-is.")
                return value
        except (ValueError, TypeError) as e:
            logger.error(f"Data type conversion error for value '{value}' to {data_type}: {e}")
            return None
    
    
    def date_filename(self, value):
        """
        Return timestamp value as a date string (for filename purposes).

        Parameters:
        - value (str or datetime): Timestamp value in either string or datetime format.

        Returns:
        - str: Formatted date value as a string.

        Raises:
        - ValueError: If the date cannot be parsed or formatted properly.
        """
        try:
            # If the value is already a datetime object
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%d")
                
            # If the value is a string, try multiple formats sequence
            elif isinstance(value, str):
                for date_format in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
                    try:
                        parsed_value = datetime.strptime(value, date_format)
                        return parsed_value.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
            else:
                logger.error(f"!! Unsupported type for date conversion: {type(value)}")
                return None
        except (ValueError, TypeError) as e:
            logger.error(f"!! Error converting value '{value}' to date format: {e}")
            return None


    def add_remark_columns(self, data, batch_id, load_dt, key):
        """
        Adds remark columns to the data for tracking and identification.

        Parameters:
        - data (dict): The input dictionary containing the raw data to which the remark columns will be added.
        - batch_id (str): A unique identifier for the ETL process batch.
        - load_dt (str): The timestamp indicating when the data was processed (format: "%Y-%m-%d %H:%M:%S").
        - key (str): A unique identifier for the city or data group.

        Returns:
        - dict: The updated dictionary with the added metadata columns.

        Raises:
        - TypeError: If the `data` parameter is not a dictionary.
        - ValueError: If `batch_id`, `load_dt`, or `key` are not provided or invalid.
        """
        if not isinstance(data, dict):
            raise TypeError("The data parameter must be a dictionary.")
        if not batch_id or not load_dt or not key:
            raise ValueError("batch_id, load_dt, and key must be provided and non-empty.")
    
        data['batch_id']   = batch_id
        data['load_dt']    = load_dt
        data['city_key']   = key

        return data


    def transform_helper(self, config, data, table_name, day:None):
        """
        Transforms and processes raw data based on the given configuration.

        Parameters:
        - config (dict): Configuration dictionary specifying the mappings, transformations, 
                        data types, and validation rules for each column.
        - data (dict): Input data dictionary containing raw data to be processed.
        - table_name (str): table name to specify data processing technique.
        - day (dict): Input forecast daily data in dictionary format to be processed (callable to process forecast data only)

        Returns:
        - dict: A dictionary representing a processed record with transformed and validated fields.

        Raises:
        - KeyError: If the specified mapping key in the configuration does not exist in the data.
        - ValueError: If a data type conversion fails or validation fails for a field.
        - Exception: If an unexpected error occurs during transformation or validation.
        """
        processed_record = {}
        timestamps_to_validate = []

        for column, column_config in config.items():
            # Checking table name
            if table_name == "forecast":
                field_value = None
                try:
                    # Handle nested key extraction properly
                    if column_config['mapping'].startswith('forecast.forecastday[].day.condition.'):
                        field_value = day['day']['condition'].get(column_config['mapping'].split('.')[-1])
                    elif column_config['mapping'].startswith('forecast.forecastday[].day.'):
                        field_value = day['day'].get(column_config['mapping'].split('.')[-1])
                    elif column_config['mapping'].startswith('forecast.forecastday[].'):
                        field_value = day.get(column_config['mapping'].split('.')[-1])
                    else:
                        field_value = data.get(column_config['mapping'])
                except Exception as e:
                    logger.error(f"!! Nested data extraction error: {e}")
            else:
                field_value = data
                for key in column_config['mapping'].split('.'):
                    field_value = field_value.get(key, None)
                    if field_value is None:
                        break
                        
            # Perform transformation if specified in config
            try:
                if column_config.get('transformation') == "extract_date":
                    field_value = field_value.split(' ')[0] if field_value else None
            except Exception as e:
                logger.error(f"!! Transformation error: {e}")

            # Convert data type using the helper function
            try:
                field_value = ETLHelper().convert_data_type(field_value, column_config.get("data_type"))
                processed_record[column] = field_value
            except Exception as e:
                logger.error(f"!! Data type conversion error: {e}")

            # Perform validation if specified in config
            try:
                if column_config.get('validation'):
                    validation_function = getattr(ValidationHelper(), column_config['validation'], None)
                    if validation_function:
                        field_value = validation_function(field_value)
            except Exception as e:
                logger.error(f"!! Validation error for {column}: {e}")

            # Collect timestamps for validation if applicable
            if column_config.get('data_type') in ['DATETIME', 'TIMESTAMP'] and column != "load_dt":
                timestamps_to_validate.append(field_value)
                        
            processed_record[column] = field_value
        
        # Perform timestamp consistency validation
        if not ValidationHelper().validate_timestamp_consistency(timestamps_to_validate):
            logger.warning(f"Timestamp inconsistency detected!! Please review the data source.")

        return processed_record
