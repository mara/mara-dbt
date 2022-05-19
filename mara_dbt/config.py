import pathlib


def manifest_file_path() -> str:
    """ The dbt manifest file, usually placed at 'target/manifest.json' """
    return str(pathlib.Path('.dbt/target/manifest.json').absolute())


def schema_name() -> str:
    """ The schema name which shall be used in the config. If not set, the default db schema will be used. """
    return None


def variables() -> dict:
    """ TBD """
    return None


# -----------------------------------------------------------------------------
# Advanced config 
# -----------------------------------------------------------------------------

def profiles_dir() -> str:
    """ The folder in which the dbt profiles are saved. If None, ~/.dbt/ is used (dbt default) """
    return '.dbt'

def profile() -> str:
    """ Which dbt profile to use. If not set the default profile will be used """
    return None
