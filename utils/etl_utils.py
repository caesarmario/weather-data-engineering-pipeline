####
## Helper file for processing data 
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime

import json
import uuid
import os
import pandas as pd

from utils.logging_utils import get_logger
from utils.validation_utils import ValidationHelper

logger = get_logger("etl_process")
class ETLHelper:

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


    def read_json(self, file_path):
        """
        Read data from a JSON file and return it as a Python dictionary.

        Args:
            file_path (str): The path to the JSON file.

        Returns:
            dict: Parsed JSON data.
        """
        with open(file_path, 'r') as file:
            return json.load(file)


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
    

    def write_csv(self, data, folder_path, subfolder_path, context, date_csv):
        """
        Write data into a structured CSV file following a year/month/day hierarchy.

        Args:
            data (list of dict): The data to write to the CSV file.
            folder_path (str): The base output directory.
            subfolder_path (str): The subfolder output directory.
            context (str): The table name used for structuring the directory.
            date_csv (str): The date string for filename in YYYY-MM-DD format.

        Returns:
            str: The file path where the CSV file was saved.
        """
        # Extract year, month, and day from the date string
        date_obj = datetime.strptime(date_csv, "%Y-%m-%d")
        year, month, day = date_obj.strftime("%Y"), date_obj.strftime("%m"), date_obj.strftime("%d")

        # Construct the directory path
        directory_path = os.path.join(folder_path, subfolder_path, context, year, month, day)

        # Ensure the directory exists
        os.makedirs(directory_path, exist_ok=True)

        # Define the file path
        file_path = os.path.join(directory_path, f"{context}_{date_csv}.csv")

        # Convert data to DataFrame and write to CSV
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        
        return file_path
    

    def write_parquet(self, data, folder_path, subfolder_path, context, date_parquet):
        """
        Write data into a structured Parquet file following a year/month/day hierarchy.

        Args:
            data (list of dict): The data to write to the Parquet file.
            folder_path (str): The base output directory.
            subfolder_path (str): The subfolder output directory.
            context (str): The table name used for structuring the directory.
            date_parquet (str): The date string for filename in YYYY-MM-DD format.

        Returns:
            str: The file path where the Parquet file was saved.
        """
        # Extract year, month, and day from the date string
        date_obj = datetime.strptime(date_parquet, "%Y-%m-%d")
        year, month, day = date_obj.strftime("%Y"), date_obj.strftime("%m"), date_obj.strftime("%d")

        # Construct the directory path
        directory_path = os.path.join(folder_path, subfolder_path, context, year, month, day)

        # Ensure the directory exists
        os.makedirs(directory_path, exist_ok=True)

        # Define the file path
        file_path = os.path.join(directory_path, f"{context}_{date_parquet}.parquet")

        # Convert data to DataFrame and write to Parquet
        df = pd.DataFrame(data)
        df.to_parquet(file_path, index=False)
        
        return file_path


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


    def read_csv(self, folder_path, subfolder_path, table_name, date_csv=None):
        """
        Read the latest data from a CSV file based on the most recent file date.

        Parameters:
        - folder_path (str): Base folder path (e.g., "output").
        - subfolder_path (str): Subfolder path (e.g., "raw").
        - table_name (str): Name of the table (e.g., "current").
        - date_csv (str, optional): The specific date to read data from in format "YYYY-MM-DD". 
                                    If not provided, it reads the latest available file.

        Returns:
        - DataFrame: Pandas DataFrame containing the CSV data.

        Raises:
        - FileNotFoundError: If no matching file is found.
        - ValueError: If the file is empty or cannot be read properly.
        """
        try:
            base_path = os.path.join(folder_path, subfolder_path, table_name)

            # If a specific date is given, construct the expected file path
            if date_csv:
                date_obj = datetime.strptime(date_csv, "%Y-%m-%d")
                year, month, day = date_obj.strftime("%Y"), date_obj.strftime("%m"), date_obj.strftime("%d")
                file_path = os.path.join(base_path, year, month, day, f"{table_name}_{date_csv}.csv")
            else:
                # Scan for the latest available CSV file
                year_dirs = sorted([d for d in os.listdir(base_path) if d.isdigit()], reverse=True)
                if not year_dirs:
                    raise FileNotFoundError(f"No year directories found for {table_name} in {base_path}")

                for year in year_dirs:
                    month_dirs = sorted([d for d in os.listdir(os.path.join(base_path, year)) if d.isdigit()], reverse=True)
                    if not month_dirs:
                        continue
                    
                    for month in month_dirs:
                        day_dirs = sorted([d for d in os.listdir(os.path.join(base_path, year, month)) if d.isdigit()], reverse=True)
                        if not day_dirs:
                            continue
                        
                        for day in day_dirs:
                            file_path = os.path.join(base_path, year, month, day, f"{table_name}_{year}-{month}-{day}.csv")
                            if os.path.exists(file_path):
                                break
                        if os.path.exists(file_path):
                            break
                    if os.path.exists(file_path):
                        break

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"No CSV file found for {table_name} in {base_path}")

            logger.info(f"Loading data from: {file_path}")
            data = pd.read_csv(file_path)

            if data.empty:
                logger.warning(f"The CSV file {file_path} is empty!")
                raise ValueError("The file is empty.")

            logger.info(f"Data successfully loaded from {file_path} with {data.shape[0]} rows and {data.shape[1]} columns.")
            return data

        except FileNotFoundError as e:
            logger.error(f"File not found error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error occurred while reading CSV file: {e}")
            raise

    
    def read_parquet(self, folder_path, subfolder_path, table_name, date_parquet=None):
        """
        Read the latest data from a Parquet file based on the most recent file date.

        Parameters:
        - folder_path (str): Base folder path (e.g., "output").
        - subfolder_path (str): Subfolder path (e.g., "raw").
        - table_name (str): Name of the table (e.g., "current").
        - date_parquet (str, optional): The specific date to read data from in format "YYYY-MM-DD". 
                                        If not provided, it reads the latest available file.

        Returns:
        - DataFrame: Pandas DataFrame containing the Parquet data.

        Raises:
        - FileNotFoundError: If no matching file is found.
        - ValueError: If the file is empty or cannot be read properly.
        """
        try:
            base_path = os.path.join(folder_path, subfolder_path, table_name)
            
            # If a specific date is given, construct the expected file path
            if date_parquet:
                date_obj = datetime.strptime(date_parquet, "%Y-%m-%d")
                year, month, day = date_obj.strftime("%Y"), date_obj.strftime("%m"), date_obj.strftime("%d")
                file_path = os.path.join(base_path, year, month, day, f"{table_name}_{date_parquet}.parquet")
            else:
                # Scan for the latest available Parquet file
                year_dirs = sorted([d for d in os.listdir(base_path) if d.isdigit()], reverse=True)
                if not year_dirs:
                    raise FileNotFoundError(f"No year directories found for {table_name} in {base_path}")

                for year in year_dirs:
                    month_dirs = sorted([d for d in os.listdir(os.path.join(base_path, year)) if d.isdigit()], reverse=True)
                    if not month_dirs:
                        continue
                    
                    for month in month_dirs:
                        day_dirs = sorted([d for d in os.listdir(os.path.join(base_path, year, month)) if d.isdigit()], reverse=True)
                        if not day_dirs:
                            continue
                        
                        for day in day_dirs:
                            file_path = os.path.join(base_path, year, month, day, f"{table_name}_{year}-{month}-{day}.parquet")
                            if os.path.exists(file_path):
                                break
                        if os.path.exists(file_path):
                            break
                    if os.path.exists(file_path):
                        break

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"No Parquet file found for {table_name} in {base_path}")

            logger.info(f"Loading data from: {file_path}")
            data = pd.read_parquet(file_path)

            if data.empty:
                logger.warning(f"The Parquet file {file_path} is empty!")
                raise ValueError("The file is empty.")

            logger.info(f"Data successfully loaded from {file_path} with {data.shape[0]} rows and {data.shape[1]} columns.")
            return data

        except FileNotFoundError as e:
            logger.error(f"File not found error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error occurred while reading Parquet file: {e}")
            raise



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
