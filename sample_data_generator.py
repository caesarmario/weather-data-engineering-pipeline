####
## Script to generate sample weather data
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime, timedelta
from pathlib import Path
from minio import Minio
from io import BytesIO

import json
import random
import time
import sys
import argparse
import os

from utils.logging_utils import logger
from utils.etl_utils import ETLHelper

class DataGenerator:

    def __init__(self, empty_rate: float, error_rate: float, credentials: dict, base_date: datetime):

        # Load helper
        self.helper = ETLHelper()

        # MinIO client setup
        endpoint         = credentials["MINIO_ENDPOINT"]
        access_key       = credentials["MINIO_ROOT_USER"]
        secret_key       = credentials["MINIO_ROOT_PASSWORD"]
        self.raw_bucket  = credentials["MINIO_BUCKET_RAW"]
        self.client      = self.helper.create_minio_conn()
        self.base_date   = base_date

        # Rates
        self.empty_rate = max(0.0, min(empty_rate, 100.0))
        self.error_rate = max(0.0, min(error_rate, 100.0))
        if self.empty_rate + self.error_rate > 100.0:
            logger.warning(
                "empty_rate + error_rate > 100; reducing error_rate"
            )
            self.error_rate = 100.0 - self.empty_rate
        
        # Load Mapping JSONs
        with open(f'schema_config/data_gen/conditions_mapping.json', 'r') as file:
            self.conditions_mapping = json.load(file)
        with open(f'schema_config/data_gen/locations_mapping.json', 'r') as file:
            self.locations_mapping = json.load(file)

        logger.info(
            f"Initialized DataGenerator with empty_rate={self.empty_rate}%, "
            f"error_rate={self.error_rate}%"
        )


    def generate_forecast_days(self, start_date: datetime, num_days: int = 3):
        """
        Function to generate a list of forecast entries for consecutive days starting at start_date.

        Args:
            - start_date: The date to begin the forecast (datetime object).
            - num_days: Number of days to generate forecasts for.

        Returns:
            A list of dictionaries matching weather API forecast structure.
        """
        forecast = []

        for day_offset in range(num_days):
            try:
                date    = start_date + timedelta(days=day_offset)
                epoch   = int(time.mktime(date.timetuple()))
                
                # Select a random condition for the day
                daily_condition = random.choice(list(self.conditions_mapping.keys()))

                # Temperature metrics (Celsius)
                maxtemp_c = round(random.uniform(20, 35), 2)
                mintemp_c = round(random.uniform(10, 20), 2)
                avgtemp_c = round((maxtemp_c + mintemp_c) / 2, 2)

                # Wind metrics: consistent conversion
                maxwind_mph = round(random.uniform(2, 15), 2)
                maxwind_kph = round(maxwind_mph * 1.60934, 2)

                # Precipitation metrics: consistent conversion
                totalprecip_mm = round(random.uniform(0, 10), 2)
                totalprecip_in = round(totalprecip_mm / 25.4, 2)

                # Snow metrics
                totalsnow_cm = round(random.uniform(0, 5), 2)

                # Visibility metrics: consistent conversion
                avgvis_km    = round(random.uniform(5, 10), 2)
                avgvis_miles = round(avgvis_km * 0.621371, 2)

                # Rain and snow chances based on precipitation
                daily_will_it_rain   = 1 if totalprecip_mm > 0 else 0
                daily_chance_of_rain = daily_will_it_rain * min(int(totalprecip_mm * 10 + random.uniform(0, 20)), 100)
                daily_will_it_snow   = 1 if totalsnow_cm > 0 else 0
                daily_chance_of_snow = daily_will_it_snow * min(int(totalsnow_cm * 20 + random.uniform(0, 10)), 100)

                # UV index based on condition
                if daily_condition == "Sunny":
                    uv = round(random.uniform(7, 11), 2)
                elif daily_condition == "Partly cloudy":
                    uv = round(random.uniform(5, 9), 2)
                else:
                    uv = round(random.uniform(1, 7), 2)

                forecast.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "date_epoch": epoch,
                    "day": {
                        "maxtemp_c": maxtemp_c,
                        "maxtemp_f": round(maxtemp_c * 9/5 + 32, 2),
                        "mintemp_c": mintemp_c,
                        "mintemp_f": round(mintemp_c * 9/5 + 32, 2),
                        "avgtemp_c": avgtemp_c,
                        "avgtemp_f": round(avgtemp_c * 9/5 + 32, 2),
                        "maxwind_mph": maxwind_mph,
                        "maxwind_kph": maxwind_kph,
                        "totalprecip_mm": totalprecip_mm,
                        "totalprecip_in": totalprecip_in,
                        "totalsnow_cm": totalsnow_cm,
                        "avgvis_km": avgvis_km,
                        "avgvis_miles": avgvis_miles,
                        "avghumidity": random.randint(60, 90),
                        "daily_will_it_rain": daily_will_it_rain,
                        "daily_chance_of_rain": daily_chance_of_rain,
                        "daily_will_it_snow": daily_will_it_snow,
                        "daily_chance_of_snow": daily_chance_of_snow,
                        "condition": {
                            "text": daily_condition,
                            "code": self.conditions_mapping[daily_condition]
                        },
                        "uv": uv
                    }
                })
            except Exception as e:
                    logger.error(f"!! Error generating forecast for day offset {day_offset} - {e}")

        return forecast

    
    def generate_location_weather(self, city: str, loc_meta: dict, base_date: datetime, forecast_days: int = 3):
        """
        Generate complete weather data for a single city, including current and forecast.

        Args:
            city: Location key/name.
            loc_meta: Metadata dictionary containing name, region, country, lat, lon, tz_id.
            base_date: The datetime for current weather snapshot.
            forecast_days: Number of days to include in forecast.

        Returns:
            A dictionary matching the overall API response for one city.
        """
        try:
            epoch_now = int(time.mktime(base_date.timetuple()))

            # Current weather values
            current_c         = round(random.uniform(15, 30), 1)
            current_condition = random.choice(list(self.conditions_mapping.keys()))

            location_data = {
                city: {
                    "location": {
                        **loc_meta,
                        "localtime_epoch": epoch_now,
                        "localtime": base_date.strftime("%Y-%m-%d %H:%M")
                    },
                    "current": {
                        "last_updated": base_date.strftime("%Y-%m-%d %H:%M"),
                        "temp_c": current_c,
                        "temp_f": round(current_c * 9/5 + 32, 1),
                        "is_day": 1,
                        "condition": {
                            "text": current_condition,
                            "code": self.conditions_mapping[current_condition]
                        }
                    },
                    "forecast": {
                        "forecastday": self.generate_forecast_days(base_date, num_days=forecast_days)
                    }
                }
            }
            return location_data
        
        except Exception as e:
            logger.error(f"!! Error generating weather for location: {city} - {e}")
            return {city: {}}


    def generate_all_weather_data(self, LOCATIONS_MAPPING, base_date: datetime, forecast_days: int = 3):
        """
        Generate weather data for all specified locations.

        Args:
            LOCATIONS_MAPPING: A dict of city names to metadata.
            base_date: Datetime for current snapshot.
            forecast_days: Number of days to forecast.

        Returns:
            Aggregated weather data for all cities.
        """
        all_data = {}

        for city, meta in LOCATIONS_MAPPING.items():
            try:
                city_data = self.generate_location_weather(city, meta, self.base_date, forecast_days)
                all_data.update(city_data)
            except Exception as e:
                logger.error(f"!! Error processing city: {city} - {e}")

        return all_data


    def inject_invalid_data(self, data: dict):
        """
        Function to inject "invalid_data" strings into a random subset of numeric metric groups
        across random forecast days (day0, day1, day2, ...).

        Metric groups: current temp, maxtemp, mintemp, avgtemp, wind, precip, snow, vis.

        Args:
            data: weather data in JSON format
        
        Returns:
            data: weather data with invalid data in JSON format
        """

        # Define metric groups as tuples of key paths
        metric_groups = [
            ("current", ["temp_c", "temp_f"]),
            ("day", ["maxtemp_c", "maxtemp_f"]),
            ("day", ["mintemp_c", "mintemp_f"]),
            ("day", ["avgtemp_c", "avgtemp_f"]),
            ("day", ["maxwind_mph", "maxwind_kph"]),
            ("day", ["totalprecip_mm", "totalprecip_in"]),
            ("day", ["totalsnow_cm"]),
            ("day", ["avgvis_km", "avgvis_miles"]),
        ]
        for city, city_data in data.items():
            try:
                # Choose random number of groups to inject (at least 1)
                num_groups    = random.randint(1, len(metric_groups))
                chosen_groups = random.sample(metric_groups, num_groups)

                # Inject into 'current' section if chosen
                for section, keys in chosen_groups:
                    if section == "current":
                        for k in keys:
                            city_data["current"][k] = "invalid_data"

                # Determine forecast forecasts length
                forecast_days = city_data.get("forecast", {}).get("forecastday", [])
                num_days      = len(forecast_days)

                if num_days > 0:
                    # Choose random number of days to inject (at least 1)
                    num_days_to_inject = random.randint(1, num_days)
                    day_indices = random.sample(range(num_days), num_days_to_inject)
                    for idx in day_indices:
                        for section, keys in chosen_groups:
                            if section == "day":
                                for k in keys:
                                    forecast_days[idx]["day"][k] = "invalid_data"
            except Exception:
                logger.exception(f"!! Error injecting invalid data for {city}")
                
        return data


    def upload_to_minio(self, data: dict, object_name: str):
        """
        Upload file to MinIO bucket (raw).

        Args:
            data: weather data in JSON format
        
        Returns:
            data: weather data with invalid data in JSON format
        """
        try:
            json_bytes = json.dumps(data, indent=4).encode("utf-8")
            stream = BytesIO(json_bytes)

            self.client.put_object(
                bucket_name=self.raw_bucket,
                object_name=object_name,
                data=stream,
                length=len(json_bytes),
                content_type="application/json"
            )

            logger.info(f"Uploaded {object_name} to bucket '{self.raw_bucket}'")
        except Exception as e:
            logger.error(f"!! Failed to upload to MinIO - {e}")

    
    def run(self):
        """
        Main entry point: generate and save sample weather data.
        """
        # Bound rates between 0-100
        try:
            empty_rate = max(0.0, min(self.empty_rate, 100.0))
            error_rate = max(0.0, min(self.error_rate, 100.0))
            if empty_rate + error_rate > 100.0:
                logger.warning("empty_rate+error_rate >100%%; adjusting error_rate")
                error_rate = max(0.0, 100.0 - empty_rate)

            logger.info(f"Rates: empty_rate={empty_rate}%%, error_rate={error_rate}%%")
        except Exception as e:
            logger.error(f"!! Failed to generate bound rates! - {e}")

        # Generate weather data
        try:
            rand_val = random.uniform(0, 100)
            if rand_val < empty_rate:
                logger.warning(f"Simulating empty JSON (rand={rand_val:.2f}<empty_rate)")
                weather_data = {}
            elif rand_val < empty_rate + error_rate:
                # Decide between injection or system error
                error_branch_val = rand_val - empty_rate
                if error_branch_val < error_rate / 2:
                    logger.warning(f"Simulating invalid fields injection (rand={rand_val:.2f}<empty+error/2)")
                    weather_data = self.generate_all_weather_data(self.locations_mapping, self.base_date, forecast_days=3)
                    weather_data = self.inject_invalid_data(weather_data)
                else:
                    logger.warning(f"Simulating system/API error JSON (rand={rand_val:.2f}>=empty+error/2)")
                    weather_data = {"api_error": 404}
            else:
                weather_data = self.generate_all_weather_data(self.locations_mapping, self.base_date, forecast_days=3)
        except Exception as e:
            logger.error(f"!! Failed to generate weather data - {e}")

        # Saving data
        try:
            object_name = f"data/weather_data_{self.base_date.strftime('%Y-%m-%d')}.json"
            self.upload_to_minio(weather_data, object_name)
            logger.info(f"Uploaded to MinIO bucket '{self.raw_bucket}', object '{object_name}'")
        except Exception as e:
            logger.error(f"!! Failed to save data - {e}")

        # If invalid or empty, exit with non-zero to signal anomaly if desired
        if not weather_data:
            logger.warning("! Generated empty or invalid weather data — no data uploaded.")
        elif "api_error" in weather_data:
            logger.warning("! Generated simulated API error data.")


def main():
    # Retrieving arguments
    try:
        parser = argparse.ArgumentParser(description="Generate sample weather data")
        parser.add_argument("--empty_rate", type=float, default=0.0, help="Pct chance to output empty JSON")
        parser.add_argument("--error_rate", type=float, default=0.0, help="Pct chance to inject errors or system err JSON")
        parser.add_argument("--credentials", type=str, required=True, help="MinIO credentials")
        parser.add_argument("--exec_date", type=str, required=True, help="Execution date in YYYY-MM-DD format")
        args = parser.parse_args()
    except Exception as e:
        logger.error(f"!! One of the arguments is empty! - {e}")

    try:
        creds         = json.loads(args.credentials)
        exec_date_str = args.exec_date
        exec_date     = datetime.strptime(exec_date_str, "%Y-%m-%d")
    except Exception as e:
        logger.error(f"!! Failed to parse JSON credentials: {e}")
        raise ValueError("!! Invalid credentials JSON format")

    try:
        gen = DataGenerator(args.empty_rate, args.error_rate, creds, exec_date)
        gen.run()
    except Exception as e:
        logger.error(f"!! Error running data generator - {e}")

if __name__ == "__main__":
    main()