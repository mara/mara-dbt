Mara dbt
========

A integration of [dbt](https://www.getdbt.com/) into the [Mara Framework](https://github.com/mara).

&nbsp;

Installation
============

To use the library directly:

```bash
pip install mara-dbt
```

dbt Project inside the Mara project
===================================

You can choose to use a dbt project inside the mara project. To do so, add the following to your `.gitignore` file

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
