####
## Python script to render DAGs and ETL scripts
##
## Author   : Mario Caesar
## LinkedIn : https://www.linkedin.com/in/caesarmario/
## Email    : caesarmario87@gmail.com
## GitHub   : https://github.com/caesarmario/weather-data-engineering-pipeline
####

# Importing Libraries
from jinja2 import Environment, FileSystemLoader

import os
import click
import yaml


def get_config(path):
    """
    Load YAML configuration file.

    Args:
        path (str): Filesystem path to the YAML config.

    Returns:
        dict: Parsed config or empty dict if file is missing or invalid.
    """
    if not os.path.isfile(path):
        click.echo(f"[WARN] Config not found: {path}", err=True)
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_template(search_path, name):
    """
    Retrieve a Jinja2 template by name.

    Args:
        search_path (str): Directory where templates reside.
        name (str): Template filename to load.

    Returns:
        jinja2.Template: Compiled template object.
    """
    env = Environment(
        loader = FileSystemLoader(os.path.abspath(search_path)),
        trim_blocks = True,
        lstrip_blocks = True,
    )
    return env.get_template(name)


@click.group()
def cli():
    """
    CLI group for rendering DAGs and scripts.
    """
    pass

# --------------------------------------------------------------------------------
# DAG rendering commands
# --------------------------------------------------------------------------------
@cli.command()
def gendag01():
    """
    Render the JSON-generation Airflow DAG.

    Writes the generated DAG to airflow/dags/01_dag...py.

    Returns:
        None
    """
    filename = "01_dag_weather_generate_json_daily"

    # Load DAG template
    tpl_dir  = "template/dag"
    tpl_name = f"{filename}.py.j2"
    template = get_template(tpl_dir, tpl_name)

    # Render and write out
    out_path = f"airflow/dags/{filename}.py"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    template.stream().dump(out_path)
    click.echo(f"Rendered DAG → {out_path}")


@cli.command()
def gendag02():
    """
    Render the Parquet-staging Airflow DAG.

    Reads processes map from template/dag/config/process_config.yaml.
    Outputs to airflow/dags/02_dag....py.

    Returns:
        None
    """
    filename = "02_dag_weather_json_to_parquet_daily"
    config   = "process_config"

    # Load DAG config
    cfg_path  = f"template/dag/config/{config}.yaml"
    cfg       = get_config(cfg_path)
    processes = cfg.get("processes", {})

    # Load DAG template
    tpl_dir  = "template/dag"
    tpl_name = f"{filename}.py.j2"
    template = get_template(tpl_dir, tpl_name)

    # Render and write out
    out_path = f"airflow/dags/{filename}.py"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    template.stream(processes=processes).dump(out_path)
    click.echo(f"Rendered DAG → {out_path}")


@cli.command()
def gendag03():
    """
    Render the Parquet-staging Airflow DAG.

    Reads processes map from template/dag/config/process_config.yaml.
    Outputs to airflow/dags/03_dag....py.

    Returns:
        None
    """
    filename = "03_dag_weather_load_parquet_daily"
    config   = "process_config"

    # Load DAG config
    cfg_path  = f"template/dag/config/{config}.yaml"
    cfg       = get_config(cfg_path)
    processes = cfg.get("processes", {})

    # Load DAG template
    tpl_dir  = "template/dag"
    tpl_name = f"{filename}.py.j2"
    template = get_template(tpl_dir, tpl_name)

    # Render and write out
    out_path = f"airflow/dags/{filename}.py"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    template.stream(processes=processes).dump(out_path)
    click.echo(f"Rendered DAG → {out_path}")

# --------------------------------------------------------------------------------
# ETL script rendering command
# --------------------------------------------------------------------------------
@cli.command()
def genscriptprocessraw():
    """
    Generate per-table ETL scripts from a Jinja template.

    Loads process definitions from template/script/config/process_config.yaml,
    and writes one Python file per entry under scripts/raw/.

    Returns:
        None
    """
    filename  = "process_raw_template"
    config    = "process_raw_config"
    
    # Load script config
    cfg_path  = f"template/script/config/{config}.yaml"
    cfg       = get_config(cfg_path)
    processes = cfg.get("processes", {})

    # Load script template
    tpl_dir  = "template/script"
    tpl_name = f"{filename}.py.j2"
    template = get_template(tpl_dir, tpl_name)

    # Loop & render one file per process
    for table_name, props in processes.items():
        out_dir  = f"scripts/process_raw"
        out_file = f"process_raw_{table_name}.py"
        os.makedirs(out_dir, exist_ok=True)

        template.stream(
            table_name=table_name,
            config_file=props.get("config_file"),
            date_cols=props.get("date_cols")
        ).dump(os.path.join(out_dir, out_file))

        click.echo(f"Rendered script → {out_dir}/{out_file}")

    click.echo("Done rendering ETL scripts.")

if __name__ == "__main__":
    cli()


############################################################################################################
'''
THANK YOU for taking the time to read through this code 🙏🏻

I hope it helps you gain some insight, spark an idea, or simply shows you a different approach to solving a data engineering challenge.

This project was built with curiosity, care, and a love for clean, scalable systems. If it helped you understand something new — even a small detail — I consider it a win 🎯

If you'd like to share feedback or just want to connect, feel free to reach out:
📧 caesarmario87@gmail.com
🔗 linkedin.com/in/caesarmario/

Keep building, keep learning 🚀

>>>> Copyright (C) 2025 - Mario Caesar <<<<
'''
############################################################################################################
