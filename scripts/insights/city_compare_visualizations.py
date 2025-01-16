####
## ETL file for creating visualizations comparing cities
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from utils.etl_helpers import ETLHelper
from utils.logging_config import logger
from utils.validation_helpers import ValidationHelper

import pandas as pd
import matplotlib.pyplot as plt

class CityComparisonVisualization:
    """
    Class for generating visualizations comparing city temperature statistics.
    """
    def __init__(self):
        try:
            # Initialize helper and set folder paths
            self.helper             = ETLHelper()
            self.validation_helper  = ValidationHelper()
            self.folder_output      = "output"
            self.subfolder_raw      = "raw"
            self.subfolder_insights = "insights"
            self.load_process_dt    = self.helper.get_load_timestamp()
            self.config             = self.helper.load_config(self.subfolder_insights, "city_compare_visualizations")
            logger.info("-- Initialized CityComparisonVisualization class successfully.")
        except Exception as e:
            logger.error(f"!! Failed to initialize CityComparisonVisualization class: {e}")
            raise

    def load_data(self):
        """
        Load the processed CSV files for forecast and current data as Pandas DataFrames.
        """
        try:
            logger.info("-- Loading forecast and current data from CSV files")
            forecast_data = self.helper.read_csv(self.folder_output, self.subfolder_raw, "forecast")
            current_data  = self.helper.read_csv(self.folder_output, self.subfolder_raw, "current")
            location_data = self.helper.read_csv(self.folder_output, self.subfolder_raw, "location")

            # Extract the process date from the data
            self.process_date = forecast_data['date'].min()

            # Merge to include city name instead of location_id
            forecast_data = forecast_data.merge(location_data[['location_id', 'name']], on='location_id')
            current_data  = current_data.merge(location_data[['location_id', 'name']], on='location_id')

            logger.info("Successfully loaded CSV files and merged with location data.")
            return forecast_data, current_data
        except Exception as e:
            logger.error(f"!! Error loading CSV files: {e}")
            raise

    def generate_visualization(self):
        """
        Generate visualizations for city comparisons.
        """
        try:
            logger.info("-- Starting city comparison visualization generation")
            forecast_data, current_data = self.load_data()

            # Convert temperature columns to numeric
            forecast_data["maxtemp_c"] = pd.to_numeric(forecast_data["maxtemp_c"], errors='coerce')
            forecast_data["mintemp_c"] = pd.to_numeric(forecast_data["mintemp_c"], errors='coerce')
            forecast_data["avgtemp_c"] = pd.to_numeric(forecast_data["avgtemp_c"], errors='coerce')
            current_data["temp_c"]     = pd.to_numeric(current_data["temp_c"], errors='coerce')

            # Grouping data to calculate average metrics for each city
            city_avg_stats = forecast_data.groupby("name").agg(
                avg_max_temp_c =("maxtemp_c", "mean"),
                avg_min_temp_c =("mintemp_c", "mean"),
                avg_temp_c     =("avgtemp_c", "mean")
            ).reset_index()

            # Calculate average current temperature for comparison
            current_avg_stats = current_data.groupby("name").agg(
                avg_current_temp_c=("temp_c", "mean")
            ).reset_index()

            # Merge the current temperature into the forecasted averages
            city_avg_stats = city_avg_stats.merge(current_avg_stats, on="name")

            for column, column_config in self.config.items():
                # Convert data type using the helper function
                try:
                    if column in city_avg_stats.columns:
                        city_avg_stats[column] = city_avg_stats[column].apply(
                            lambda value: self.helper.convert_data_type(value, column_config.get("data_type"))
                        )
                except Exception as e:
                    logger.error(f"!! Data type conversion error for column '{column}': {e}")

                # Perform validation if specified in config
                try:
                    if column_config.get('validation'):
                        validation_function = getattr(self.validation_helper, column_config['validation'], None)
                        if validation_function:
                            city_avg_stats[column] = city_avg_stats[column].apply(validation_function)  # Apply validation

                except Exception as e:
                    logger.error(f"!! Validation error for {column} in comparison result: {e}")

            # Generate bar chart for temperature comparison with a better color palette
            plt.figure(figsize=(14, 8))
            bar_width = 0.2
            index     = range(len(city_avg_stats))

            colors    = ['#4E79A7', '#F28E2B', '#E15759', '#76B7B2']

            bars1 = plt.bar(index, city_avg_stats["avg_current_temp_c"], width=bar_width, label="Avg Current Temp (°C)", color=colors[0])
            bars2 = plt.bar([p + bar_width for p in index], city_avg_stats["avg_max_temp_c"], width=bar_width, label="Avg Max Temp (°C)", color=colors[1])
            bars3 = plt.bar([p + bar_width * 2 for p in index], city_avg_stats["avg_min_temp_c"], width=bar_width, label="Avg Min Temp (°C)", color=colors[2])
            bars4 = plt.bar([p + bar_width * 3 for p in index], city_avg_stats["avg_temp_c"], width=bar_width, label="Avg Forecast Temp (°C)", color=colors[3])

            # Add annotations on top of each bar
            for bars in [bars1, bars2, bars3, bars4]:
                for bar in bars:
                    plt.annotate(f"{bar.get_height():.1f}", xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()), 
                                 ha='center', va='bottom')

            plt.xlabel("City")
            plt.ylabel("Temperature (°C)")
            plt.title(f"City Temperature Comparison (Current vs Forecast) as per {self.process_date}")
            plt.xticks([p + bar_width * 1.5 for p in index], city_avg_stats["name"], rotation=45)
            plt.legend()
            plt.tight_layout()

            # Generate Output
            full_output_folder  = f"{self.folder_output}/{self.subfolder_insights}"
            output_file         = f"{full_output_folder}/city_temperature_comparison_{self.process_date}.png"
            plt.savefig(output_file)
            plt.close()
            logger.info(f"City comparison visualization completed successfully and saved to {output_file}")

        except Exception as e:
            logger.error(f"!! Error during city comparison visualization process: {e}")
            raise

if __name__ == "__main__":
    try:
        logger.info("-- Starting city comparison visualization script")
        visualizer = CityComparisonVisualization()
        visualizer.generate_visualization()
        logger.info("-- City comparison visualization script completed successfully")
    except Exception as e:
        logger.error(f"!! Critical error running the city comparison visualization script: {e}")
