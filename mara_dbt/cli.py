import click
import yaml
import pathlib

from .setup import generate_project_file, generate_profile_file

DBT_PROEJCT_FILENAME = 'dbt_project.yml'
DBT_PROFILES_FILENAME = '.dbt/profiles.yml'


@click.command()
def setup():
    dbt_folders = [
        '.dbt',
        '.dbt/target',
        '.dbt/logs',
        'dbt',
        'dbt/analysis',
        'dbt/macros',
        'dbt/models',
        'dbt/snapshots',
        'dbt/tests'
    ]

    for dbt_folder in dbt_folders:
        pathlib.Path(dbt_folder).mkdir(parents=True, exist_ok=True)

    dbt_project = generate_project_file()
    with open(pathlib.Path(DBT_PROEJCT_FILENAME).absolute(),'w') as f:
        yaml.dump(dbt_project, f)

    profile = generate_profile_file()
    with open(pathlib.Path(DBT_PROFILES_FILENAME).absolute(),'w') as f:
        yaml.dump(profile, f)
