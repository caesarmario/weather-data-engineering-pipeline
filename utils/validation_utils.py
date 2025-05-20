####
## Utils file for validation purposes
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime, timedelta
from utils.logging_utils import logger

# WIP - 20250520 START
import json
import logging

from minio import Minio
from io import BytesIO
# WIP - 20250520 END

class ValidationHelper:

    # WIP - 20250520
    def validate_raw_json(data: dict) -> bool:
        """
        Validate raw JSON:
            - Must be dict non-empty
            - Must have no "api_error" key
            - Every city contain "location", "current", "forecast"
        """
        if not isinstance(data, dict):
            logger.error("Raw JSON not dict: %r", data)
            return False
        if not data:
            logger.error("Raw JSON is empty")
            return False
        if "api_error" in data:
            logger.error("Raw JSON contains api_error: %s", data["api_error"])
            return False
        for city, city_data in data.items():
            if not all(k in city_data for k in ("location", "current", "forecast")):
                logger.error("City %s missing required section", city)
                return False
        return True

    # Validation functions
    def validate_latitude(self, lat):
        """
        Validates latitude values to ensure they are within valid range (-90 to 90).

        Args:
            lat (float): Latitude value.

        Returns:
            float: Validated latitude or None if invalid.
        """
        try:
            lat = float(lat)
            if -90 <= lat <= 90:
                return lat
            else:
                return None
        except (ValueError, TypeError):
            return None


    def validate_longitude(self, lon):
        """
        Validates longitude values to ensure they are within valid range (-180 to 180).

        Args:
            lon (float): Longitude value.

        Returns:
            float: Validated longitude or None if invalid.
        """
        try:
            lon = float(lon)
            if -180 <= lon <= 180:
                return lon
            else:
                return None
        except (ValueError, TypeError):
            return None


    def validate_timezone(self, tz_id):
        """
        Validates timezone strings to ensure they follow expected format.

        Args:
            tz_id (str): Timezone string.

        Returns:
            str: Validated timezone or None if invalid.
        """
        if isinstance(tz_id, str) and tz_id.strip():
            return tz_id
        return None


    def validate_date(self, date_str):
        """
        Validates date strings to ensure they follow "YYYY-MM-DD" format.

        Args:
            date_str (str): Date string.

        Returns:
            str: Validated date or None if invalid.
        """
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except (ValueError, TypeError):
            return None


    def validate_timestamp(self, timestamp_str):
        """
        Validates and parses a timestamp string.

        Args:
            timestamp_str (str): The timestamp string to validate.

        Returns:
            str: The original timestamp string if valid.
            None: If the timestamp is invalid.
        """
        try:
            datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M")
            return timestamp_str
        except ValueError:
            return None


    def validate_temperature_celsius(self, temp_c):
        """
        Validates temperature in Celsius with a reasonable range.

        Parameters:
        - temp_c (float): Temperature value in Celsius to validate.

        Returns:
        - float or None: Valid temperature value or None if invalid.
        """
        try:
            temp_c = float(temp_c)
            if -89.2 <= temp_c <= 56.7:
                return temp_c
            else:
                raise ValueError("Temperature in Celsius out of realistic range.")
        except (ValueError, TypeError) as e:
            print(e)
            return None


    def validate_temperature_fahrenheit(self, temp_f):
        """
        Validates temperature in Fahrenheit with a reasonable range.

        Parameters:
        - temp_f (float): Temperature value in Fahrenheit to validate.

        Returns:
        - float or None: Valid temperature value or None if invalid.
        """
        try:
            temp_f = float(temp_f)
            if -128.6 <= temp_f <= 134:
                return temp_f
            else:
                raise ValueError("Temperature in Fahrenheit out of realistic range.")
        except (ValueError, TypeError):
            return None


    def validate_boolean(self, value):
        """
        Validate if the value is a valid boolean (0 or 1).

        Args:
            value: The boolean value to validate.

        Returns:
            bool: True or False if valid, None otherwise.
        """
        try:
            if value in [0, 1, True, False]:
                return bool(value)
            return None
        except (ValueError, TypeError):
            return None


    def validate_humidity(self, humidity):
        """
        Validates humidity value to ensure it's within the range of 0 to 100.

        Parameters:
        - humidity (int or float): Humidity value to validate.

        Returns:
        - int or None: Valid humidity value or None if invalid.
        """
        try:
            humidity = int(humidity)
            if 0 <= humidity <= 100:
                return humidity
            else:
                raise ValueError("Humidity value out of valid range (0-100).")
        except (ValueError, TypeError):
            return None
    
    
    def validate_timestamp_consistency(self, timestamps, max_difference_minutes=15):
        """
        Validates if timestamps fall within the acceptable time difference threshold.

        Parameters:
        - timestamps (list): List of datetime objects to compare.
        - max_difference_minutes (int): Maximum allowed time difference in minutes.
        
        Returns:
        - bool: True if all timestamps are within the specified threshold, False otherwise.
        """
        try:
            # If list is empty, pass the process
            if not timestamps:
                return True
            
            # Convert string timestamps to datetime if necessary
            timestamps = [datetime.strptime(ts, "%Y-%m-%d %H:%M") if isinstance(ts, str) else ts for ts in timestamps]
            
            # Identify the earliest and latest timestamps
            earliest_timestamp = min(timestamps)
            latest_timestamp = max(timestamps)
            
            # Calculate the time difference
            time_difference = latest_timestamp - earliest_timestamp

            # Check if the difference exceeds the threshold
            if time_difference >= timedelta(minutes=max_difference_minutes):
                logger.warning(f"Timestamp inconsistency detected! Difference exceeds {max_difference_minutes} minutes.")
                return False
            return True

        except Exception as e:
            logger.error(f"Error validating timestamp consistency: {e}")
            return False