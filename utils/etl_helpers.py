####
## Helper file for processing JSON 
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime

import json
import uuid
import os
import pandas as pd

from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

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
        subfolder = f"{subfolder}_config"

        with open(f'config/{subfolder}/{config_name}.json', 'r') as file:
            return json.load(file)


    def write_csv(self, data, folder_path, subfolder_path, context, date_csv):
        """
        Write data into a CSV file using pandas DataFrame.

        Args:
            data (list of dict): The data to write to the CSV file.
            file_path (str): The destination file path for the CSV file.

        Returns:
            None
        """
        file_path = f'{folder_path}/{subfolder_path}/{context}_{date_csv}.csv'

        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
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


    def read_csv(self, folder_path, subfolder_path, table_name):
        """
        Read the latest data from a CSV file based on the most recent file date.

        Parameters:
        - folder_path (str): Path to the folder containing CSV files.
        - table_name (str): Name of the table to locate the correct file.

        Returns:
        - DataFrame: Pandas DataFrame containing the CSV data.

        Raises:
        - FileNotFoundError: If no matching file is found.
        - ValueError: If the file is empty or cannot be read properly.
        """
        try:
            full_folder_path = f"{folder_path}/{subfolder_path}"
            logger.info(f"Searching for the latest CSV file for {table_name} in {full_folder_path}")
            
            # List all files in the folder
            all_files = [f for f in os.listdir(full_folder_path) if f.startswith(table_name) and f.endswith(".csv")]

            if not all_files:
                raise FileNotFoundError(f"No CSV files found for {table_name} in {full_folder_path}")

            # Sort files by date using the date in the filename (assuming format table_name_YYYY-MM-DD.csv)
            latest_file = sorted(all_files, key=lambda x: x.split('_')[-1].split('.')[0], reverse=True)[0]
            file_path = os.path.join(full_folder_path, latest_file)

            # Load the data
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

    
    def add_remark_columns(self, data, batch_id, load_dt, key):
        
        data['batch_id']   = batch_id
        data['load_dt']    = load_dt
        data['city_key']   = key

        return data


    def transform_helper(self, config, data):
            
        processed_record = {}
        timestamps_to_validate = []

        for column, column_config in config.items():
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

