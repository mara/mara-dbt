from functools import singledispatch
import mara_db.config
import mara_pipelines.config
from mara_db import dbs

from . import config


@singledispatch
def profile_target_config(db: object):
    """ Generates the profile target config part for a mara database config """
    return None # by default we ignore not supported dbs

@profile_target_config.register(dbs.PostgreSQLDB)
def __(db: dbs.PostgreSQLDB):
    return {
        'type': 'postgres',
        'host': db.host,
        'user': db.user,
        'password': db.password,
        #'port': db.port if db.port else 1433,
        'dbname': db.database,
        'schema': config.schema_name() if config.schema_name() else 'public',
        'threads': 1,
        'keepalives_idle': 0,
        'sslmode': db.sslmode
    }

@profile_target_config.register(dbs.RedshiftDB)
def __(db: dbs.RedshiftDB):
    return {
        'type': 'redshift',
        'host': db.host,
        'user': db.user,
        'password': db.password,
        'port': 5439,
        'dbname': db.database,
        'schema': config.schema_name() if config.schema_name() else 'analysis',
        'threads': 4,
        'keepalives_idle': 0,
        'sslmode': db.sslmode
    }

@profile_target_config.register(dbs.BigQueryDB)
def __(db: dbs.BigQueryDB):
    return {
        'type': 'bigquery',
        'method': 'oauth',
        'project': db.project,
        'dataset': config.schema_name() if config.schema_name() else db.dataset,
        'threads': 1,
        'timeout_seconds': 300,
        'location': db.location,
        'priority': 'interactive',
        'retries': 1
    }

@profile_target_config.register(dbs.SQLServerDB)
def __(db: dbs.SQLServerDB):
    return {
        'type': 'sqlserver',
        'driver': db.odbc_driver,
        'server': db.host,
        'port': db.port if db.port else 1433,
        'user': db.user,
        'password': db.password,
        'database': db.database,
        'schema': config.schema_name() if config.schema_name() else 'dbo'
    }


def generate_profile_file():
    """
    Generates the .dbt/profiles.yml file based on the local mara config
    """
    output_targets: dict = {}

    for db_alias, db in mara_db.config.databases().items():
        target_config = profile_target_config(db)
        if target_config:
            output_targets[db_alias] = target_config

    profile = {
        'config': {
            'partial_parse': True,
            'use_colors': False,
            # we set this to fales because if activated it slows down the execution process
            'send_anonymous_usage_stats': False
        },

        # this is the profile to be used in the mara project
        'mara': {
            # will use the local mara files
            'target': mara_pipelines.config.default_db_alias(),
            'outputs': output_targets
        }
    }

    return profile


def generate_project_file():
    """
    Generates the default dbt_project.yml file for mara
    """
    project = {
        'name': 'mara',
        'version': '1.0.0',
        'config-version': 2,
        'profile': 'mara',

        # path configs
        'source-paths': ['dbt/models'],
        'analysis-paths': ["dbt/analysis"],
        'test-paths': ["dbt/tests"],
        'data-paths': ["data"],
        'macro-paths': ["dbt/macros"],
        'snapshot-paths': ["dbt/snapshots"],
        'log-path': ".dbt/logs",
        'target-path': ".dbt/target",
        'clean-targets': [
            ".dbt/target",
            "dbt/dbt_modules"
        ],

        # models config
        'models': {
            '+materialized': 'view'
        }
    }

    return project
