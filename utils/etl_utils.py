####
## Utilities for ETL operations
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime
from io import BytesIO
from minio import Minio
from psycopg2 import OperationalError, sql

import json
import uuid
import os
import pandas as pd
import psycopg2

from utils.logging_utils import logger
from utils.validation_utils import ValidationHelper

class ETLHelper:
    """
    Helper for common ETL tasks
    """
    
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


    def create_minio_conn(self, credentials):
        """
        Function to create a MinIO client.
        
        Args:
            credentials (dict): MinIO credentials.

        Returns:
            MinIO client connection.
        """
        endpoint         = credentials["MINIO_ENDPOINT"]
        access_key       = credentials["MINIO_ROOT_USER"]
        secret_key       = credentials["MINIO_ROOT_PASSWORD"]

        return Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )


    def read_json(self, bucket_name, object_name: str, credentials):
        """
        Read JSON from MinIO raw bucket, parse, dan return dict.

        Args:
            object_name (str): Object name in MinIO raw bucket to be read as JSON

        Returns:
            dict: parsed JSON data
        """
        try:
            minio_client = self.create_minio_conn(credentials)
            resp         = minio_client.get_object(bucket_name, object_name) # Fetch the JSON file from the MinIO bucket
            data         = json.loads(resp.read().decode("utf-8"))
            return data

        except Exception as e:
            logger.error(f"!! Failed to read JSON from MinIO bucket '{bucket_name}', object '{object_name}': {e}")
            raise

        finally:
            # Close and release the connection
            if 'resp' in locals() and resp is not None:
                try:
                    resp.close()
                    resp.release_conn()
                except Exception:
                    pass


    def load_config(self, subfolder, config_name):
        """
        Load configuration from a JSON file.

        Args:
            subfolder (str): Subdirectory under schema_config.
            config_name (str): The name of configuration file.

        Returns:
            dict: Configuration data loaded from the file.
        """

        with open(f'schema_config/{subfolder}/{config_name}.json', 'r') as file:
            return json.load(file)


    def upload_parquet_to_minio(self, data, context, bucket, date_parquet, credentials):
        """
        Convert records to Parquet and upload to MinIO staging bucket.

        Args:
            data (list[dict]): Transformed records.
            context (str): Table/context name for path.
            bucket (str): Target bucket name.
            date_parquet (str): Date string 'YYYY-MM-DD' for file naming.
            credentials (str): MinIO credentials.

        Returns:
            str: Uploaded object path in bucket.
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

            # Create MinIO connection
            minio_client = self.create_minio_conn(credentials)

            # Upload
            minio_client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            return object_name
        except Exception as e:
            logger.error(f"!! Failed to upload Parquet file to MinIO bucket '{bucket}', object '{object_name}': {e}")
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

        Args:
            value (str or datetime): Timestamp value in either string or datetime format.

        Returns:
            str: Formatted date value as a string.
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
            data (dict): The input dictionary containing the raw data to which the remark columns will be added.
            batch_id (str): A unique identifier for the ETL process batch.
            load_dt (str): The timestamp indicating when the data was processed (format: "%Y-%m-%d %H:%M:%S").
            key (str): A unique identifier for the city or data group.

        Returns:
            dict: The updated dictionary with the added metadata columns.
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
            config (dict): Configuration dictionary specifying the mappings, transformations, 
                            data types, and validation rules for each column.
            data (dict): Input data dictionary containing raw data to be processed.
            table_name (str): table name to specify data processing technique.
            day (dict): Input forecast daily data in dictionary format to be processed (callable to process forecast data only)

        Returns:
            dict: A dictionary representing a processed record with transformed and validated fields.
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


    def read_parquet(self, bucket_name, credentials, table_name, exec_date):
        """
        Read a Parquet file from a MinIO bucket, parse it, and return it as a DataFrame.

        Args:
            bucket_name (str): The name of the MinIO bucket.
            credentials (dict): A dictionary containing MinIO credentials.
            table_name (str): The name of the table or the context for the Parquet file (e.g., "current").
            exec_date (str): The execution date in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the data from the Parquet file
        """
        try:
            # Split the exec_date
            year, month, day = exec_date.split('-')

            # Construct the path to the Parquet file
            path = (
                f"{table_name}/"
                f"{year}/{month}/{day}/"
                f"{table_name}_{exec_date}.parquet"
            )

            minio_client = self.create_minio_conn(credentials)
            resp         = minio_client.get_object(bucket_name, path) # Fetch the Parquet file from the MinIO bucket

            byte_data    = BytesIO(resp.read()) # Read the Parquet data into memory
            df           = pd.read_parquet(byte_data)

            return df

        except Exception as e:
            logger.error(f"!! Failed to read Parquet from MinIO bucket '{bucket_name}', path '{path}': {e}")
            raise
            
        finally:
            # Close and release the connection
            if 'resp' in locals() and resp is not None:
                try:
                    resp.close()
                    resp.release_conn()
                except Exception:
                    pass

    
    def create_postgre_conn(self, postgres_creds):
        """
        ################## TO DO 20250610
        Initialize and return a SQLAlchemy engine for Postgres connections.

        Args:
            postgres_creds (dict): Dictionary containing connection parameters:

        Returns:
            sqlalchemy.Engine: A SQLAlchemy engine instance connected to the specified Postgres database.
        """
        try:
            user           = postgres_creds["POSTGRES_USER"]
            password       = postgres_creds["POSTGRES_PASSWORD"]
            host           = postgres_creds["POSTGRES_HOST"]
            port           = postgres_creds["POSTGRES_PORT"]
            dbname         = postgres_creds["POSTGRES_DB"]

            conn = psycopg2.connect(
                database=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )

            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                logger.info("Successfully connected to PostgreSQL database.")
            return conn

        except Exception as e:
            logger.error(f"!! Creating postgres connection failed: {e}")
            raise

    def load_reserved_keywords(self):
        """
        ################## TO DO 20250610
        Load configuration from a JSON file.

        Args:
            subfolder (str): Subdirectory under schema_config.
            config_name (str): The name of configuration file.

        Returns:
            dict: Configuration data loaded from the file.
        """

        try:
            # Load reserve keywords
            with open(f'utils/reserved_keywords.json', 'r') as file:
                return json.load(file)["postgres_reserved_keywords"]
        except Exception as e:
            logger.error(f"!! Failed to load reserve keywords: {e}")
            raise


    def check_and_create_table(self, conn, table_name: str, schema: str, df: pd.DataFrame):
        """
        ################## TO DO 20250610
        Check if the table exists in the given schema, and create it if it doesn't.

        Args:
            conn (psycopg2.connection): The psycopg2 connection object to interact with the database.
            table_name (str): The name of the table.
            schema (str): The schema where the table is located.
            df (pandas.DataFrame): DataFrame with the data to be inserted or used to create the table.
        """
        try:
            # Create a cursor to execute SQL queries
            with conn.cursor() as cursor:

                # Check if the table exists in the schema by querying the information_schema.tables
                cursor.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = %s)"),
                    [table_name, schema]
                )
                exists = cursor.fetchone()[0]  # Fetch the result of the query, which is a boolean
                
                if not exists:
                    logger.info(f"Table {schema}.{table_name} does not exist. Creating table...")

                    # Generate the SQL columns based on the config file and pandas DataFrame
                    config = self.load_config(schema, f"{table_name}_config")

                    # Map pandas data types to PostgreSQL data types
                    dtype_mapping = {
                        'STRING': 'TEXT',
                        'DATE': 'DATE',
                        'DATETIME': 'TIMESTAMP',
                        'FLOAT': 'DOUBLE PRECISION',
                        'BOOLEAN': 'BOOLEAN',
                        'INTEGER': 'INTEGER',
                    }

                    # Load reserved keywords
                    reserved_keywords = self.load_reserved_keywords()

                    columns = []
                    for column, column_config in config.items():
                        # Get the column name and its data type from the config

                        column_name = f'"{column}"' if column.lower() in reserved_keywords else column
                        column_data_type = column_config['data_type']
                        
                        # Map the data type to a PostgreSQL type
                        pg_data_type = dtype_mapping.get(column_data_type, 'TEXT')
                        
                        # Add the column to the list of SQL columns
                        columns.append(f"{column_name} {pg_data_type}")

                    # Print the columns first (before joining)
                    columns_sql = ", ".join(columns)

                    # Create the SQL query to create the table
                    create_table_query = f"CREATE TABLE {schema}.{table_name} ({columns_sql})"
                    logger.info(f"Generated SQL Query: {create_table_query}")

                    # Execute the create table query
                    cursor.execute(create_table_query)
                    conn.commit()
                    logger.info(f"Table {schema}.{table_name} created successfully. Proceed to next step")
                else:
                    logger.info(f"Table {schema}.{table_name} already exists. Proceed to next step!")

        except Exception as e:
            logger.error(f"Error in checking or creating table {schema}.{table_name}: {e}")
            raise


################## TO DO 20250610
#     def check_and_create_table(self, conn, table_name: str, schema: str, df: pd.DataFrame):
#     """
#     ################## TO DO 20250610
#     Check if the table exists in the given schema, and create it if it doesn't.

#     Args:
#         conn (psycopg2.connection): The psycopg2 connection object to interact with the database.
#         table_name (str): The name of the table.
#         schema (str): The schema where the table is located.
#         df (pandas.DataFrame): DataFrame with the data to be inserted or used to create the table.
#     """
#     try:
#         with conn.cursor() as cursor:
#             # Check if the table exists
#             cursor.execute(
#                 sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = %s)"),
#                 [table_name, schema]
#             )
#             exists = cursor.fetchone()[0]  # Fetch the result of the query

#             if not exists:
#                 logger.info(f"Table {schema}.{table_name} does not exist. Creating table...")
#                 self.create_table(conn, table_name, schema, df)
#             else:
#                 logger.info(f"Table {schema}.{table_name} already exists. Checking for schema updates...")
#                 # Perform checks for data type and column differences
#                 self.check_and_update_schema(conn, table_name, schema)

#     except Exception as e:
#         logger.error(f"Error in checking or creating table {schema}.{table_name}: {e}")
#         raise


# def create_table(self, conn, table_name: str, schema: str, df: pd.DataFrame):
#     """
#     Create a new table based on the provided configuration.
#     """
#     try:
#         config = self.load_config(schema, f"{table_name}_config")
#         dtype_mapping = {
#             'STRING': 'TEXT',
#             'DATE': 'DATE',
#             'DATETIME': 'TIMESTAMP',
#             'FLOAT': 'DOUBLE PRECISION',
#             'BOOLEAN': 'BOOLEAN',
#             'INTEGER': 'INTEGER',
#         }

#         reserved_keywords = self.load_reserved_keywords()
#         columns = []
#         for column, column_config in config.items():
#             column_name = f'"{column}"' if column.lower() in reserved_keywords else column
#             column_data_type = column_config['data_type']
#             pg_data_type = dtype_mapping.get(column_data_type, 'TEXT')
#             columns.append(f"{column_name} {pg_data_type}")

#         columns_sql = ", ".join(columns)
#         create_table_query = f"CREATE TABLE {schema}.{table_name} ({columns_sql})"
#         logger.info(f"Generated SQL Query: {create_table_query}")

#         with conn.cursor() as cursor:
#             cursor.execute(create_table_query)
#             conn.commit()
#             logger.info(f"Table {schema}.{table_name} created successfully.")

#     except Exception as e:
#         logger.error(f"Error creating table {schema}.{table_name}: {e}")
#         raise


# def check_and_update_schema(self, conn, table_name: str, schema: str):
#     """
#     Check if there are any data type changes or new columns, and apply necessary updates.
#     """
#     try:
#         config = self.load_config(schema, f"{table_name}_config")
#         with conn.cursor() as cursor:
#             cursor.execute(
#                 sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s AND table_schema = %s"),
#                 [table_name, schema]
#             )
#             existing_columns = cursor.fetchall()
#             existing_columns = {col[0]: col[1] for col in existing_columns}

#             # Check for data type changes and add new columns
#             self.check_and_update_columns(conn, table_name, schema, existing_columns, config)

#     except Exception as e:
#         logger.error(f"Error checking and updating schema {schema}.{table_name}: {e}")
#         raise


# def check_and_update_columns(self, conn, table_name: str, schema: str, existing_columns: dict, config: dict):
#     """
#     Check if columns exist in the table, and if their data types match the configuration.
#     """
#     try:
#         dtype_mapping = {
#             'STRING': 'TEXT',
#             'DATE': 'DATE',
#             'DATETIME': 'TIMESTAMP',
#             'FLOAT': 'DOUBLE PRECISION',
#             'BOOLEAN': 'BOOLEAN',
#             'INTEGER': 'INTEGER',
#         }

#         reserved_keywords = self.load_reserved_keywords()
        
#         # Check for columns to be added and data types to be updated
#         with conn.cursor() as cursor:
#             for column, column_config in config.items():
#                 column_name = column
#                 column_data_type = column_config['data_type']
#                 pg_data_type = dtype_mapping.get(column_data_type, 'TEXT')

#                 if column_name not in existing_columns:
#                     logger.info(f"Adding new column {column_name} to {schema}.{table_name}")
#                     cursor.execute(
#                         sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
#                             sql.Identifier(schema),
#                             sql.Identifier(table_name),
#                             sql.Identifier(column_name),
#                             sql.SQL(pg_data_type)
#                         )
#                     )
#                     conn.commit()
#                     logger.info(f"Added column {column_name} to {schema}.{table_name}")
#                 elif existing_columns[column_name] != pg_data_type:
#                     logger.info(f"Altering column {column_name} in {schema}.{table_name} to {pg_data_type}")
#                     cursor.execute(
#                         sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET DATA TYPE {}").format(
#                             sql.Identifier(schema),
#                             sql.Identifier(table_name),
#                             sql.Identifier(column_name),
#                             sql.SQL(pg_data_type)
#                         )
#                     )
#                     conn.commit()
#                     logger.info(f"Updated column {column_name} data type to {pg_data_type} in {schema}.{table_name}")

#     except Exception as e:
#         logger.error(f"Error checking and updating columns in {schema}.{table_name}: {e}")
#         raise


    # def merge_data_into_table(conn, df: pd.DataFrame, table_name: str, schema: str, exec_date: str):
    #     """
    #     ################## TO DO 20250610
    #     Insert data into the table by creating a temp table, merging the data and cleaning up.
    #     """
    #     temp_table_name = f"current_temp_{exec_date.replace('-', '_')}"
        
    #     try:
    #         # Create a temporary table
    #         df.head(0).to_sql(
    #             name=temp_table_name,
    #             con=conn,
    #             schema=schema,
    #             if_exists='replace',  # Creates the temp table
    #             index=False
    #         )
    #         logger.info(f"Temporary table {schema}.{temp_table_name} created.")

    #         # Insert data into the temporary table
    #         df.to_sql(
    #             name=temp_table_name,
    #             con=conn,
    #             schema=schema,
    #             if_exists='append',
    #             index=False
    #         )
    #         logger.info(f"Inserted data into temp table {schema}.{temp_table_name}.")
            
    #         # Now, merge the data into the main table (assuming no unique conflicts on primary key)
    #         merge_sql = f"""
    #         INSERT INTO raw.{table_name} (SELECT * FROM raw.{temp_table_name})
    #         ON CONFLICT (your_primary_key_column) DO UPDATE
    #         SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, ... ;
    #         """
            
    #         # Execute the merge SQL
    #         with conn.begin():
    #             conn.execute(text(merge_sql))
            
    #         logger.info(f"Successfully merged data from temp table {schema}.{temp_table_name} into {schema}.{table_name}.")

    #         # Drop the temporary table after merging
    #         conn.execute(f"DROP TABLE IF EXISTS raw.{temp_table_name};")
    #         logger.info(f"Dropped temporary table {schema}.{temp_table_name}.")
        
    #     except Exception as e:
    #         logger.error(f"Error during merging process: {e}")
    #         raise