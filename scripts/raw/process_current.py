####
## ETL file for processing current weather data
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

# Processing data
class ProcessCurrent:
    def __init__(self):
        # Setting up the variables and config
        try:
            self.helper            = ETLHelper()
            self.validation_helper = ValidationHelper()
            self.batch_id          = self.helper.generate_batch_id()
            self.config            = self.helper.load_config("raw", "current_config")
            self.load_dt           = self.helper.get_load_timestamp()
            self.folder_path       = "output"
            self.subfolder_path    = "raw"
            self.table_name        = "current"

            logger.info("Configuration for current table loaded successfully")
        except Exception as e:
            logger.error(f"!! Failed to load configuration: {e}")
            raise

    def process(self, data):
        # Main processing
        logger.info("- Starting the data processing for current table...")
        processed_data = []
        timestamps_to_validate = []

        try:
            for city_key, city_data in data.items():
                try:
                    # Add batch ID, load timestamp, and city key
                    city_data['batch_id']   = self.batch_id
                    city_data['load_dt']    = self.load_dt
                    city_data['city_key']   = city_key

                    # Extract the columns based on the configuration
                    processed_record = {}
                    
                    for column, column_config in self.config.items():
                        field_value = city_data
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
                            field_value = self.helper.convert_data_type(field_value, column_config.get("data_type"))
                            processed_record[column] = field_value
                        except Exception as e:
                            logger.error(f"!! Data type conversion error: {e}")

                        # Perform validation if specified in config
                        try:
                            if column_config.get('validation'):
                                validation_function = getattr(self.validation_helper, column_config['validation'], None)
                                if validation_function:
                                    field_value = validation_function(field_value)
                        except Exception as e:
                            logger.error(f"!! Validation error for {column}: {e}")

                        # Collect timestamps for validation if applicable
                        if column_config.get('data_type') in ['DATETIME', 'TIMESTAMP'] and column != "load_dt":
                            timestamps_to_validate.append(field_value)
                        
                        processed_record[column] = field_value

                    # Perform timestamp consistency validation
                    if not self.validation_helper.validate_timestamp_consistency(timestamps_to_validate):
                        logger.warning(f"Timestamp inconsistency detected!! Please review the data source.")

                    processed_data.append(processed_record)
                    logger.info(f"Successfully processed data for city: {city_key}")
                except Exception as e:
                    # Log errors for individual city processing
                    logger.error(f"!! Error processing data for city: {city_key}, Error: {e}")
        except Exception as e:
            # Log unexpected errors during iteration
            logger.error(f"!! Unexpected error during data iteration: {e}")
            raise

        try:
            # Generate output filename using the current date
            date_csv    = self.helper.date_filename(processed_data[0]['last_updated'])
            output_file = f'{self.folder_path}/{self.subfolder_path}/{self.table_name}_{date_csv}.csv'

            # Write processed data to a CSV file
            self.helper.write_csv(processed_data, output_file)
            logger.info(f"Data successfully written to {output_file}")
        except Exception as e:
            # Log errors during file writing
            logger.error(f"!! Error writing data to CSV: {e}")
            raise
