"""
Python script to render ETL and Flatten Process files using Jinja templates and YAML configurations.
Mario Caesar // caesarmario87@gmail.com
"""

# Importing Libraries
import yaml
import jinja2
import os
import argparse

from utils.logging_helpers import get_logger

logger = get_logger("file_render")

class ScriptRenderer:
    def __init__(self, config_file, mode):
        """
        Initializes the script renderer for both ETL transformations and flattening process scripts.

        Parameters:
        - config_file (str): YAML configuration file containing script definitions.
        - mode (str): Defines whether to render 'etl', 'process', or 'all'.
        """
        self.template_dir   = "template"
        self.config_folder  = "config"
        self.output_dirs    = {
            "etl"    : "scripts/etl",
            "process": "scripts/raw"
        }
        self.templates      = {
            "etl"    : "etl_template.py.j2",
            "process": "process_template.py.j2"
        }

        self.mode          = mode
        self.config_dir    = os.path.join(self.template_dir, self.config_folder)
        self.config_file   = os.path.join(self.config_dir, config_file)
        self.config        = self.load_config()

        # Ensure output directories exist
        for key, path in self.output_dirs.items():
            os.makedirs(path, exist_ok=True)

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

    def render_scripts(self):
        """
        Renders Python scripts dynamically using Jinja2 templates based on the selected mode.
        """
        try:
            # Load Jinja environment
            template_loader = jinja2.FileSystemLoader(searchpath=self.template_dir)
            template_env    = jinja2.Environment(loader=template_loader)

            if self.mode in ["etl", "all"]:
                self.render_etl(template_env)

            if self.mode in ["process", "all"]:
                self.render_process(template_env)

        except Exception as e:
            logger.error(f"!! Error during script rendering: {e}")
            raise

    def render_etl(self, template_env):
        """
        Renders ETL scripts based on Jinja2 template and YAML config.
        """
        template = template_env.get_template(self.templates["etl"])

        for etl_name, etl_config in self.config.get("etl_processes", {}).items():
            output_file = os.path.join(self.output_dirs["etl"], f"etl_{etl_name}.py")

            rendered_script = template.render(
                etl_name        = etl_name,
                input_sources   = etl_config["input_sources"],
                transformations = etl_config["transformations"],
                output_format   = etl_config["output_format"]
            )

            with open(output_file, "w") as f:
                f.write(rendered_script)

            logger.info(f">> Generated ETL script: {output_file}")

    def render_process(self, template_env):
        """
        Renders Flatten Process scripts based on Jinja2 template and YAML config.
        """
        template = template_env.get_template(self.templates["process"])

        for process_name, process_config in self.config.get("processes", {}).items():
            output_file = os.path.join(self.output_dirs["process"], f"process_{process_name}.py")

            rendered_script = template.render(
                process_name    = process_name,
                input_sources   = process_config["input_sources"],
                mappings        = process_config["mappings"],
                output_format   = process_config["output_format"]
            )

            with open(output_file, "w") as f:
                f.write(rendered_script)

            logger.info(f">> Generated Flatten Process script: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Render Python scripts for ETL and Flatten Process from Jinja templates and YAML config.")
    parser.add_argument(
        "--config", 
        type     = str, 
        required = True, 
        help     = "Specify the YAML config file (inside 'template/config/')"
    )
    parser.add_argument(
        "--mode", 
        type     = str, 
        choices  = ["etl", "process", "all"],
        default  = ["all"],
        required = True, 
        help     = "Specify whether to render 'etl', 'process', or 'all'"
    )

    args = parser.parse_args()

    try:
        renderer = ScriptRenderer(args.config, args.mode)
        renderer.render_scripts()
    except Exception as e:
        logger.error(f"!! Script rendering failed: {e}")
