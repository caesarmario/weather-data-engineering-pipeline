####
## Utilities for ETL operations
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime
from io import BytesIO
from minio import Minio
from psycopg2 import OperationalError, sql
from psycopg2.extras import execute_values

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
        Initialize and return a pyscopg2 engine for Postgres connections.

        Args:
            postgres_creds (dict): Dictionary containing connection parameters:

        Returns:
            psycopg2 conn: A psycopg2 connection connected to the specified Postgres database.
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
                
                # Check if the schema exists in the database
                cursor.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)"),
                    [schema]
                )
                schema_exists = cursor.fetchone()[0]  # Fetch the result of the query, which is a boolean

                if not schema_exists:
                    logger.info(f"Schema {schema} does not exist. Creating schema...")
                    try:
                        cursor.execute(
                            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
                        )
                        conn.commit()
                        logger.info(f"Schema {schema} created successfully.")
                    except Exception as e:
                        logger.error(f"Error creating schema {schema}: {e}")
                        if "duplicate key" in str(e):  # Ignore error if schema already exists
                            logger.info(f"Schema {schema} already exists. Skipping creation.")


                # Check if the table exists in the schema by querying the information_schema.tables
                cursor.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = %s)"),
                    [table_name, schema]
                )
                table_exists = cursor.fetchone()[0]  # Fetch the result of the query, which is a boolean
                
                if not table_exists:
                    logger.info(f"Table {schema}.{table_name} does not exist. Creating table...")

                    # Generate the SQL columns based on the config file and pandas DataFrame
                    config = self.load_config(schema, f"{table_name}_config")

                    # Load reserved keywords
                    reserved_keywords = self.load_reserved_keywords()

                    columns = []
                    for column, column_config in config.items():
                        # Get the column name and its data type from the config

                        column_name = f'"{column}"' if column.lower() in reserved_keywords else column
                        column_data_type = column_config['data_type']
                        
                        # Map the data type to a PostgreSQL type
                        pg_data_type = self._map_dtype_to_postgres_from_config(column_data_type)
                        
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
                    # Checking schema changes if exists
                    logger.info(f"Table {schema}.{table_name} already exists. Checking for schema changes...")
                    self.alter_table_schema(conn, table_name, schema, df)

        except Exception as e:
            logger.error(f"Error in checking or creating table {schema}.{table_name}: {e}")
            raise
    

    def _map_dtype_to_postgres(self, dtype):
        """
        Map pandas data types to equivalent PostgreSQL data types.

        Args:
            dtype (numpy.dtype or str): The data type from a pandas DataFrame column.
                Expected common types include:
                - 'string'
                - 'object'
                - 'float64'
                - 'int64'
                - 'bool'
                - 'datetime64[ns]'
                - 'datetime'
                - 'date'

        Returns:
            str: PostgreSQL-compatible data type string. Defaults to 'TEXT' if no match found.
        """
        dtype_mapping = {
            'string': 'TEXT',
            'datetime64[ns]': 'TIMESTAMP',
            'float64': 'DOUBLE PRECISION',
            'bool': 'BOOLEAN',
            'int64': 'INTEGER',
            'object': 'TEXT',
            'datetime': 'TIMESTAMP',
            'date': 'DATE'
        }
        
        return dtype_mapping.get(str(dtype), 'TEXT')


    def _map_dtype_to_postgres_from_config(self, column_data_type):
        """
        Map abstract data types (as defined in config files) to PostgreSQL-compatible data types.

        Args:
            column_data_type (str): Logical or abstract data type defined in the schema config file.
                Supported types (case-sensitive):
                    - 'STRING'     → 'TEXT'
                    - 'DATE'       → 'DATE'
                    - 'DATETIME'   → 'TIMESTAMP'
                    - 'TIMESTAMP'  → 'TIMESTAMP'
                    - 'FLOAT'      → 'DOUBLE PRECISION'
                    - 'BOOLEAN'    → 'BOOLEAN'
                    - 'INTEGER'    → 'INTEGER'

        Returns:
            str: Corresponding PostgreSQL data type. Defaults to 'TEXT' if no match is found.
        """
        dtype_mapping = {
            'STRING': 'TEXT',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'FLOAT': 'DOUBLE PRECISION',
            'BOOLEAN': 'BOOLEAN',
            'INTEGER': 'INTEGER',
            'TIMESTAMP': 'TIMESTAMP'
        }
        
        return dtype_mapping.get(column_data_type, 'TEXT')


    def alter_table_schema(self, conn, table_name: str, schema: str, df: pd.DataFrame):
        """
        Alter the table schema by adding new columns or updating data types of existing columns
        to match the given DataFrame.

        Args:
            conn (psycopg2.connection): The psycopg2 connection object to interact with the database.
            table_name (str): The name of the table.
            schema (str): The schema where the table is located.
            df (pandas.DataFrame): DataFrame with the data to be inserted or used to alter the table schema.
        """
        try:
            with conn.cursor() as cursor:
                # Get existing columns and data types from the database
                cursor.execute(
                    sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s AND table_schema = %s"),
                    [table_name, schema]
                )
                existing_columns = {row[0]: row[1] for row in cursor.fetchall()}

                # Get new columns from DataFrame
                new_columns = df.columns.tolist()

                # Add new columns or alter existing columns
                for column in new_columns:
                    pg_data_type = self._map_dtype_to_postgres(df[column].dtype)

                    if column not in existing_columns:
                        logger.info(f"Adding new column {column} with type {pg_data_type} to {schema}.{table_name}.")
                        cursor.execute(
                            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {};").format(
                                sql.Identifier(schema),
                                sql.Identifier(table_name),
                                sql.Identifier(column),
                                sql.SQL(pg_data_type)
                            )
                        )
                        conn.commit()
                        logger.info(f"Column {column} added to {schema}.{table_name}.")
                    elif existing_columns[column].upper() != pg_data_type.upper():
                        logger.info(f"Updating column {column} from {existing_columns[column]} to {pg_data_type}.")
                        cursor.execute(
                            sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET DATA TYPE {};").format(
                                sql.Identifier(schema),
                                sql.Identifier(table_name),
                                sql.Identifier(column),
                                sql.SQL(pg_data_type)
                            )
                        )
                        conn.commit()
                        logger.info(f"Column {column} updated to {pg_data_type} in {schema}.{table_name}.")

        except Exception as e:
            logger.error(f"Error in altering table schema for {schema}.{table_name}: {e}")
            raise


    def truncate_insert_data(self, conn, table_name: str, schema: str, df: pd.DataFrame, exec_date: str):
        """
        Truncate-insert approach to load transformed data into a target PostgreSQL table by ensuring
        duplicate business keys are removed and leverage temporary table.

        Args:
            conn (psycopg2.connection): PostgreSQL connection.
            table_name (str): Target table name for inserting cleaned data.
            schema (str): Target schema (e.g., l0_weather, l1_weather).
            df (pandas.DataFrame): Cleaned and transformed data to be loaded.
            exec_date (str): Execution date (used for temp table naming).
        """
        try:
            exec_date = datetime.now().strftime("%Y%m%d")
            temp_table_name = f"{table_name}_temp_{exec_date}"
            load_dt = datetime.now()

            # Add `load_dt`` to df
            df["load_dt"] = load_dt

            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

            with conn.cursor() as cursor:
                # Load reserved keywords config
                config = self.load_config(schema, f"{table_name}_config")
                reserved_keywords = self.load_reserved_keywords()

                # Create column definitions
                columns = []
                for column, column_config in config.items():
                    # Quote reserved keywords
                    column_name = f'"{column}"' if column.lower() in reserved_keywords else column
                    column_data_type = column_config['data_type']

                    # Map the data type to a PostgreSQL type
                    pg_data_type = self._map_dtype_to_postgres_from_config(column_data_type)
                    columns.append(f"{column_name} {pg_data_type}")

                # SQL query to create the temporary table
                delete_temp_table_query = f"DROP TABLE IF EXISTS {schema}.{temp_table_name};"
                cursor.execute(delete_temp_table_query)
                logger.info(f">> Dropping existing temp table: {delete_temp_table_query.strip()}")

                create_temp_table_query = f"CREATE TABLE {schema}.{temp_table_name} ({', '.join(columns)});"
                logger.info(f">> Creating temp table: {create_temp_table_query.strip()}")
                cursor.execute(create_temp_table_query)

                # Insert into temp table
                insert_query = f"INSERT INTO {schema}.{temp_table_name} ({', '.join([f'"{col}"' if col.lower() in reserved_keywords else col for col in df.columns])}) VALUES %s"
                insert_values = [tuple(row) for row in df.values]
                logger.info(f">> Inserting into temp table...: {insert_query.strip()}")
                execute_values(cursor, insert_query, insert_values)

                # Deleting old records based on condition
                if "date" in df.columns:
                    # If the table has a `date` column
                    delete_query = f"""
                        DELETE FROM {schema}.{table_name}
                        WHERE (location_id, date::DATE) IN (
                            SELECT location_id, date::DATE FROM {schema}.{temp_table_name}
                        )
                    """
                    logger.info(f">> Deleting old records... {delete_query.strip()}")
                else:
                    # If the table has `localtime` - location table
                    delete_query = f"""
                        DELETE FROM {schema}.{table_name}
                        WHERE (location_id, localtime) IN (
                            SELECT location_id, localtime FROM {schema}.{temp_table_name}
                        )
                    """
                    logger.info(f">> Deleting old records... {delete_query.strip()}")
                cursor.execute(delete_query)

                # Insert from temp to main table
                column_list = ", ".join([f'"{col}"' if col.lower() in reserved_keywords else col for col in df.columns])
                insert_main_query = f"""
                    INSERT INTO {schema}.{table_name} ({column_list})
                    SELECT {column_list}
                    FROM {schema}.{temp_table_name};
                """
                logger.info(f">> Inserting to main table... {insert_main_query.strip()}")
                cursor.execute(insert_main_query)

                # Drop temp table
                cursor.execute(f"DROP TABLE IF EXISTS {schema}.{temp_table_name};")

            conn.commit()
            logger.info(f">> Truncate insert process complete for {schema}.{table_name}")

        except Exception as e:
            logger.error(f"!! Error during upsert for {schema}.{table_name}: {e}")
            raise