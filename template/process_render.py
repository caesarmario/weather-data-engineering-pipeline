####
## ETL file for rendering flatten process script
## Mario Caesar // caesarmario87@gmail.com
####

import os
import yaml
import argparse

from jinja2 import Environment, FileSystemLoader

# Define the renderer
def render_process_files(template_file, yaml_file, output_dir):
    """
    Render process files from a Jinja template and a YAML configuration.

    Parameters:
    - template_file (str): Path to the Jinja template file.
    - yaml_file (str): Path to the YAML configuration file.
    - output_dir (str): Directory where rendered files will be saved.

    Returns:
    None
    """
    try:
        # Load the Jinja environment
        env = Environment(loader=FileSystemLoader(os.path.dirname(template_file)))
        template = env.get_template(os.path.basename(template_file))

        # Load YAML configuration
        with open(yaml_file, "r") as file:
            config = yaml.safe_load(file)

        # Process each source in the YAML file
        for source in config["source"]:
            for table_name, params in source.items():
                print(table_name, params)
                # config_name = params["config_name"]
                # date_csv_col = params["date_csv_col"]

                # # Render the template
                # rendered_content = template.render(
                #     process_name=f"Process{table_name.capitalize()}",
                #     table_name=table_name,
                #     config_name=config_name,
                #     date_csv_col=date_csv_col
                # )

                # # Save the rendered file
                # output_file = os.path.join(output_dir, f"process_{table_name}.py")
                # with open(output_file, "w") as file:
                #     file.write(rendered_content)
                # print(f"Generated: {output_file}")

    except Exception as e:
        print(f"Error rendering process files: {e}")

# Define CLI arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Render process files from Jinja template and YAML configuration.")
    parser.add_argument("--template", required=True, help="Path to the Jinja template file.")
    parser.add_argument("--config", required=True, help="Path to the YAML configuration file.")
    parser.add_argument("--output", required=True, help="Directory to save the rendered files.")
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output, exist_ok=True)

    # Run the renderer
    render_process_files(args.template, args.config, args.output)
