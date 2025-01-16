####
## Main ETL Runner for Weather Data Processing
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

from scripts.raw.process_location import ProcessLocation
from scripts.raw.process_current import ProcessCurrent
from scripts.raw.process_forecast import ProcessForecast

from scripts.insights.compare_temperatures import CompareTemperatures
from scripts.insights.identify_highest_temperature import IdentifyHighestTemperature
from scripts.insights.temperature_statistics_by_city import TemperatureStatistics
from scripts.insights.city_compare_visualizations import CityComparisonVisualization
from scripts.insights.weather_alerts import WeatherAlerts

class MainETLRunner:
    """
    Main ETL Runner Class to manage the execution of all ETL processes.
    """
    def __init__(self, data_path):
        try:
            self.helper                         = ETLHelper()
            self.validation_helper              = ValidationHelper()
            self.data_path                      = data_path
            self.location_processor             = ProcessLocation()
            self.current_processor              = ProcessCurrent()
            self.forecast_processor             = ProcessForecast()
            self.comparator                     = CompareTemperatures()
            self.highest_temp_identifier        = IdentifyHighestTemperature()
            self.temperature_stats_calculator   = TemperatureStatistics()
            self.visualizer                     = CityComparisonVisualization()
            self.weather_alert_identifier       = WeatherAlerts()
            logger.info("ETL Runner initialized successfully.")
        except Exception as e:
            logger.error(f"Error during ETL Runner initialization: {e}")
            raise

    def run_etl(self):
        """
        Execute the entire ETL pipeline in the following sequence:
        1. Flatten and store current and forecast data from JSON.
        2. Compare current temperature with forecasted data.
        3. Identify the day with the highest temperature for each city.
        4. Calculate minimum, maximum, and average temperatures for each city.
        5. Create visualizations comparing city temperatures.
        6. Identify and flag weather alerts based on predefined thresholds.
        """
        logger.info("-- STARTING THE ETL PIPELINE --")

        try:
            # Flatten and store weather data
            logger.info("Flattening and storing data...")
            json_data = self.helper.read_json(self.data_path)
            self.location_processor.process(json_data)
            self.current_processor.process(json_data)
            self.forecast_processor.process(json_data)
            logger.info("Flattening process completed successfully.")

            # # Compare current temperature with forecasted data
            # logger.info("Comparing current vs forecast temperatures...")
            # self.comparator.compare_temperatures()
            # logger.info("Temperature comparison completed.")

            # # Identify the day with the highest temperature for each city
            # logger.info("Identifying the highest temperature day per city...")
            # self.highest_temp_identifier.identify_highest_temperature()
            # logger.info("Highest temperature identification completed.")

            # # Calculate minimum, maximum, and average temperatures for each city
            # logger.info("Calculating temperature statistics...")
            # self.temperature_stats_calculator.calculate_temperature_statistics()
            # logger.info("Temperature statistics calculated successfully.")

            # # Generate city temperature comparison visualizations
            # logger.info("Generating city temperature visualizations...")
            # self.visualizer.generate_visualization()
            # logger.info("City comparison visualization completed.")

            # # Identify and flag weather alerts
            # logger.info("Identifying weather alerts...")
            # self.weather_alert_identifier.identify_weather_alerts()
            # logger.info("Weather alert identification completed.")

            logger.info("-- ETL PIPELINE COMPLETED SUCCESSFULLY --")

        except Exception as e:
            logger.error(f"Critical error during ETL pipeline execution: {e}")
            raise

    @staticmethod
    def main():
        """
        Static method to trigger the ETL runner execution.
        """
        try:
            logger.info("Launching the ETL pipeline...")
            etl_runner = MainETLRunner('data/weather_data_sample.json')
            etl_runner.run_etl()
        except Exception as e:
            logger.error(f"Critical error during ETL execution: {e}")
            raise

if __name__ == "__main__":
    MainETLRunner.main()
