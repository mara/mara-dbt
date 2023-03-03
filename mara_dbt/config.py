import pathlib
import os
from typing import Optional


def dbt_target() -> Optional[str]:
    """
    The dbt default target to be used.

    See as well:
      * https://docs.getdbt.com/docs/faqs/target-names
      * https://docs.getdbt.com/guides/legacy/managing-environments
    """
    return None


def dbt_variables() -> dict:
    """
    Variables to be passed to all dbt commands.

    They can be overriden in a mara command.

    See as well:
      * https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables
    """
    return {}


def dbt_cloud_host() -> Optional[str]:
    """
    dbt Cloud host (cloud.getdbt.com (multi-tenant instance) by default if the environment variable is not set)
    """
    return None


def dbt_cloud_api_token() -> Optional[str]:
    """
    API authentication key
    """
    return None


def dbt_cloud_account_id() -> Optional[str]:
    """
    Numeric ID of the dbt Cloud account
    """
    return None


# -----------------------------------------------------------------------------
# Advanced config
# -----------------------------------------------------------------------------

def profiles_dir() -> Optional[str]:
    """ The folder in which the dbt profiles are saved. If None, ~/.dbt/ is used (dbt default) """
    return None


def profile() -> Optional[str]:
    """ Which dbt profile to use. If not set the default profile will be used """
    return None


def project_dir() -> Optional[str]:
    """ If a custom project path shall be used. This is read from environment variable DBT_PROJECT_DIR. """
    # Use env. to support https://github.com/dbt-labs/dbt-core/issues/6078. Can be dropped when minimum
    # dbt version for this module is a version which supports dbt-core#6078.
    return os.environ.get('DBT_PROJECT_DIR')


# -----------------------------------------------------------------------------
# Experimental, building dbt via the mara project
# -----------------------------------------------------------------------------

def schema_name() -> Optional[str]:
    """ The schema name which shall be used in the config. If not set, the default db schema will be used. """
    return None


def manifest_file_path() -> str:
    """ The dbt manifest file, usually placed at 'target/manifest.json' """
    return str(pathlib.Path('.dbt/target/manifest.json').absolute())
