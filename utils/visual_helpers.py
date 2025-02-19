####
## Helper file for visualization processing
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from datetime import datetime

import os
import matplotlib.pyplot as plt

from utils.logging_helpers import get_logger

logger = get_logger("etl_process")


class VisualHelper:
    def __init__(self):
        """
        Initializes the VisualHelper class.
        """
        self.folder_path = "visuals"


    def create_directory(self, subfolder):
        """
        Creates directory structure based on the visualization type and date.

        Args:
            subfolder (str): The subfolder name for the visualization.

        Returns:
            str: The full path to the directory.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        directory_path = os.path.join(self.folder_path, subfolder, today)

        os.makedirs(directory_path, exist_ok=True)
        return directory_path


    def save_plot(self, fig, filename, subfolder):
        """
        Saves the given Matplotlib figure to the appropriate directory.

        Args:
            fig (matplotlib.figure.Figure): The figure to save.
            filename (str): The filename for the output image.
            subfolder (str): The subfolder name for the visualization.

        Returns:
            str: The saved file path.
        """
        try:
            directory = self.create_directory(subfolder)
            file_path = os.path.join(directory, f"{filename}.png")

            fig.savefig(file_path, bbox_inches="tight")
            plt.close(fig)

            logger.info(f"Visualization saved: {file_path}")
            return file_path
        
        except Exception as e:
            logger.error(f"Error saving visualization: {e}")
            return None

    def plot_temperature_comparison(self, df, city_column, temp_column, subfolder="temperature_comparison"):
        """
        Creates a bar chart comparing temperatures across cities.

        Args:
            df (pd.DataFrame): Dataframe containing city and temperature data.
            city_column (str): Column name for city names.
            temp_column (str): Column name for temperature values.
            subfolder (str, optional): Subfolder to store the visualization. Defaults to "temperature_comparison".

        Returns:
            str: The path to the saved visualization.
        """
        try:
            fig, ax     = plt.subplots(figsize=(10, 5))
            df_sorted   = df.sort_values(by=temp_column, ascending=False)

            ax.barh(df_sorted[city_column], df_sorted[temp_column], color="skyblue")

            ax.set_xlabel("Temperature (°C)")
            ax.set_ylabel("City")
            ax.set_title("Temperature Comparison Across Cities")
            
            plt.gca().invert_yaxis()

            return self.save_plot(fig, "temperature_comparison", subfolder)
        except Exception as e:
            logger.error(f"Error generating temperature comparison chart: {e}")
            return None
