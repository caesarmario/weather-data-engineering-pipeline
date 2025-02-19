"""
Python script to render ETL process files using Jinja templates and YAML configurations.
Mario Caesar // caesarmario87@gmail.com
"""

# Importing Libraries
import yaml
import jinja2
import os
import argparse

from utils.logging_helper import get_logger

logger = get_logger("file_render")

class ProcessRenderer:
    def __init__(self, config_file):
        """
        Initializes the renderer with paths and configurations.

        Parameters:
        - config_file (str): YAML configuration file containing process definitions.
        """
        self.template_dir   = "template"
        self.config_folder  = "config"
        self.output_dir     = "scripts/raw"
        self.jinja_template = "process_template.py.j2"

        self.template_file  = os.path.join(self.template_dir, self.jinja_template)
        self.config_dir     = os.path.join(self.template_dir, self.config_folder)
        self.config_file    = os.path.join(self.config_dir, config_file)
        self.config         = self.load_config()

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    def load_config(self):
        """
        Loads the YAML configuration file.

        Returns:
        - dict: Parsed YAML content.

        Raises:
        - FileNotFoundError: If the config file is missing.
        - yaml.YAMLError: If there's an error parsing the YAML file.
        """
        try:
            with open(self.config_file, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"!! Error: Config file '{self.config_file}' not found in {self.config_dir}.")
            raise
        except yaml.YAMLError as e:
            logger.error(f"!! Error parsing YAML file: {e}")
            raise

    def render_process(self):
        """
        Renders Python process files dynamically using Jinja2 templates.
        """
        try:
            # Load Jinja template
            template_loader = jinja2.FileSystemLoader(searchpath=self.template_dir)
            template_env    = jinja2.Environment(loader=template_loader)
            template        = template_env.get_template(self.jinja_template)

            # Generate process files based on YAML config
            for process_name, process_config in self.config["processes"].items():
                output_file = os.path.join(self.output_dir, f"process_{process_name}.py")

                # Render template with provided configurations
                rendered_script   = template.render(
                    table_name    = process_config["table_name"],
                    config_file   = process_config["config_file"],
                    date_csv_cols = process_config["date_csv_cols"]
                )

                # Write rendered content to file
                with open(output_file, "w") as f:
                    f.write(rendered_script)

                logger.info(f">> Generated: {output_file}")
        except Exception as e:
            logger.error(f"!! Error during rendering process: {e}")
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Render Python process files from Jinja templates and YAML config.")
    parser.add_argument(
        "--config", 
        type     = str, 
        required = True, 
        help     = "Specify the YAML config file (inside 'template/config/')"
    )

    args = parser.parse_args()

    try:
        renderer = ProcessRenderer(args.config)
        renderer.render_process()
    except Exception as e:
        logger.error(f"!! Process rendering failed: {e}")
