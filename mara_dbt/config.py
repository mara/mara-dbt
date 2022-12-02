import pathlib


def dbt_target() -> str:
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


def dbt_cloud_host() -> str:
    """
    dbt Cloud host (cloud.getdbt.com (multi-tenant instance) by default if the environment variable is not set)
    """
    return None


def dbt_cloud_api_token() -> str:
    """
    API authentication key
    """
    return None


def dbt_cloud_account_id() -> str:
    """
    Numeric ID of the dbt Cloud account
    """
    return None


# -----------------------------------------------------------------------------
# Advanced config 
# -----------------------------------------------------------------------------

def profiles_dir() -> str:
    """ The folder in which the dbt profiles are saved. If None, ~/.dbt/ is used (dbt default) """
    return None

def profile() -> str:
    """ Which dbt profile to use. If not set the default profile will be used """
    return None


# -----------------------------------------------------------------------------
# Experimental, building dbt via the mara project
# -----------------------------------------------------------------------------

def schema_name() -> str:
    """ The schema name which shall be used in the config. If not set, the default db schema will be used. """
    return None


def manifest_file_path() -> str:
    """ The dbt manifest file, usually placed at 'target/manifest.json' """
    return str(pathlib.Path('.dbt/target/manifest.json').absolute())
