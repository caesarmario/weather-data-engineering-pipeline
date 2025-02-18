"""
Main ETL Runner for Weather Data Processing
Mario Caesar // caesarmario87@gmail.com
"""

import argparse

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
        """
        Initialize the ETL Runner with necessary components.

        :param data_path: Path to the JSON data file.
        """
        logger.info("-- ETL Runner initialization... --")
        self.data_path = data_path
        self.helper = ETLHelper()
        self.validation_helper = ValidationHelper()
        self.location_processor = ProcessLocation()
        self.current_processor = ProcessCurrent()
        self.forecast_processor = ProcessForecast()
        self.comparator = CompareTemperatures()
        self.highest_temp_identifier = IdentifyHighestTemperature()
        self.temperature_stats_calculator = TemperatureStatistics()
        self.visualizer = CityComparisonVisualization()
        self.weather_alert_identifier = WeatherAlerts()
        logger.info("-- ETL Runner initialized successfully. --")

    def run_etl(self, processes=None):
        """
        Execute the ETL pipeline. If no specific processes are provided, run all processes.

        :param processes: List of processes to run. If None, run all processes.
        """
        logger.info("-- Starting ETL Pipeline --")

        try:
            if processes is None or "all" in processes:
                self._flatten_and_store_data()
                self._compare_temperatures()
                self._identify_highest_temperature()
                self._calculate_temperature_statistics()
                self._generate_visualizations()
                self._identify_weather_alerts()
            else:
                if "flatten" in processes:
                    self._flatten_and_store_data()
                if "compare" in processes:
                    self._compare_temperatures()
                if "highest_temp" in processes:
                    self._identify_highest_temperature()
                if "stats" in processes:
                    self._calculate_temperature_statistics()
                if "visualize" in processes:
                    self._generate_visualizations()
                if "alerts" in processes:
                    self._identify_weather_alerts()

            logger.info("-- ETL Pipeline COMPLETED!! --")
        except Exception as e:
            logger.error(f"Critical error during ETL pipeline execution: {e}")
            raise

    def _flatten_and_store_data(self):
        """Flatten and store weather data."""
        json_data = self.helper.read_json(self.data_path)
        self.location_processor.process(json_data)
        self.current_processor.process(json_data)
        self.forecast_processor.process(json_data)

    def _compare_temperatures(self):
        """Compare current temperature with forecasted data."""
        self.comparator.compare_temperatures()

    def _identify_highest_temperature(self):
        """Identify the day with the highest temperature for each city."""
        self.highest_temp_identifier.identify_highest_temperature()

    def _calculate_temperature_statistics(self):
        """Calculate minimum, maximum, and average temperatures for each city."""
        self.temperature_stats_calculator.calculate_temperature_statistics()

    def _generate_visualizations(self):
        """Generate city temperature comparison visualizations."""
        self.visualizer.generate_visualization()

    def _identify_weather_alerts(self):
        """Identify and flag weather alerts."""
        self.weather_alert_identifier.identify_weather_alerts()

def parse_arguments():
    """
    Parse command-line arguments.

    :return: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Run the ETL pipeline for weather data processing.")
    parser.add_argument(
        "--processes",
        nargs="+",
        choices=["all", "flatten", "compare", "highest_temp", "stats", "visualize", "alerts"],
        default=["all"],
        help="List of processes to run. Default is 'all'.",
    )
    parser.add_argument(
        "--data_path",
        type=str,
        default="data/weather_data_sample.json",
        help="Path to the JSON data file. Default is 'data/weather_data_sample.json'.",
    )
    return parser.parse_args()

def main():
    """
    Main function to trigger the ETL runner execution.
    """
    try:
        args = parse_arguments()
        logger.info("Launching the ETL pipeline...")
        etl_runner = MainETLRunner(args.data_path)
        etl_runner.run_etl(args.processes)
    except Exception as e:
        logger.error(f"Critical error during ETL execution: {e}")
        raise

if __name__ == "__main__":
    main()