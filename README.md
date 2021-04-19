Mara dbt
========

A lightweight integration of [dbt](https://www.getdbt.com/) into the mara framework.

:warning: This package is under development.

&nbsp;

Installation
============

*WIP*

To use the library directly:

```bash
pip install git+https://github.com/mara/mara-dbt.git
```

Add the following to your `.gitignore` file

``` .gitignore
# dbt
/.dbt/.user.yml
/.dbt/logs/
/.dbt/profiles.yml
/.dbt/target/
/dbt/dbt_modules/
```

and `import mara_dbt` in your `app/__init__.py` file to make sure that the cli commands are recognized by your mara app.

Then execute the following shell commands in order to complete the installation:

``` shell
source .venv/bin/activate
flask mara_dbt.setup
```

When using a git repository you should commit the files shown in `git status`.

&nbsp;

Documentation
=============

*WIP*

This package will generate a dbt project `mara` in your mara project root path with a dbt profile using dbs configured in `mara_db.config.dbs`.

### Sample Pipeline

``` python
import pathlib

from mara_pipelines.pipelines import Pipeline
from mara_dbt.integration import load_manifest, add_nodes_from_manifest


pipeline = Pipeline(
    id="dbt",
    description="Pipeline executing the dbt models",
    base_path=pathlib.Path(__file__).parent)

manifest = load_manifest()
add_nodes_from_manifest(pipeline, manifest, add_model_tests=False)

```
