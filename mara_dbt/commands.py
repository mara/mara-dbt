import json
from mara_page import _
from mara_pipelines.pipelines import Command

from . import config


class _DbtCommand(Command):
    def __init__(self, dbt_verb: str, db_alias: str = None, variables: dict = None):
        """
        Executes a dbt command

        Args
            dbt_verb: the dbt command
            db_alias: the target db alias
            variables: # TODO NOT IMPLEMENTED!!!
                       Supply variables to the project. This argument
                       overrides variables defined in config.variables()
        """
        super().__init__()
        self._dbt_verb = dbt_verb
        self.variables = variables # TODO: implement variables
        self.db_alias = db_alias

    def shell_command(self):
        return (f'dbt {self._dbt_verb}'
                + (f' --profiles-dir {config.profiles_dir()}' if config.profiles_dir() else '')
                + (f' --profile {config.profile()}' if config.profile() else '')
                + (f' --target {self.db_alias}' if self.db_alias else ''))


class DbtSeed(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
                 selector: str = None, full_refresh: bool = False,
                 variables: dict = None):
        """
        Executes dbt seed

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: Drop existing seed tables and recreate them
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.variables()
        """
        super().__init__('seed', variables=variables)
        self.models = models
        self.exclude_models = exclude_models
        self.selector = selector
        self.full_refresh = full_refresh

    def shell_command(self):
        command = super().shell_command()
        models = ' '.join(self.models) if self.models else None
        exclude_models = ' '.join(self.exclude_models) if self.exclude_models else None
        command += (' -x'
                   + (f' --select {models}' if self.models else '')
                   + (f' --exclude {exclude_models}' if self.exclude_models else '')
                   + (f' --selector {self.selector}' if self.selector else '')
                   + (' --full-refresh' if self.full_refresh else ''))
        return command

    def html_doc_items(self) -> [(str, str)]:
        return [
            ('db', _.tt[self.db_alias]),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None),
            ('full refresh', _.tt[self.full_refresh])
        ]


class DbtSnapshot(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
        selector: str = None, variables: dict = None):
        """
        Executes dbt snapshot

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.variables()
        """
        super().__init__('run', variables=variables)
        self.models = models
        self.exclude_models = exclude_models
        self.selector = selector

    def shell_command(self):
        command = super().shell_command()
        models = ' '.join(self.models) if self.models else None
        exclude_models = ' '.join(self.exclude_models) if self.exclude_models else None
        command += (' -x'
                   + (f' --models {models}' if self.models else '')
                   + (f' --exclude {exclude_models}' if self.exclude_models else '')
                   + (f' --selector {self.selector}' if self.selector else ''))
        return command

    def html_doc_items(self) -> [(str, str)]:
        return [
            ('db', _.tt[self.db_alias]),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None)
        ]


class DbtRun(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
        selector: str = None, full_refresh: bool = False,
        variables: dict = None):
        """
        Executes dbt run

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: If specified, DBT will drop incremental models and
                          fully-recalculate the incremental table from the model
                          definition.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.variables()
        """
        super().__init__('run', variables=variables)
        self.models = models
        self.exclude_models = exclude_models
        self.selector = selector
        self.full_refresh = full_refresh

    def shell_command(self):
        command = super().shell_command()
        models = ' '.join(self.models) if self.models else None
        exclude_models = ' '.join(self.exclude_models) if self.exclude_models else None
        command += (' -x'
                   + (f' --models {models}' if self.models else '')
                   + (f' --exclude {exclude_models}' if self.exclude_models else '')
                   + (f' --selector {self.selector}' if self.selector else '')
                   + (' --full-refresh' if self.full_refresh else ''))
        return command

    def html_doc_items(self) -> [(str, str)]:
        return [
            ('db', _.tt[self.db_alias]),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None),
            ('full refresh', _.tt[self.full_refresh])
        ]


class DbtTest(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
        selector: str = None, data_tests: bool = False, schema_tests: bool = False,
        variables: dict = None):
        """
        Executes dbt test

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            data_tests: Run data tests defined in "tests" directory.
            schema_tests: Run constraint validations from schema.yml files
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.variables()
        """
        super().__init__('test', variables=variables)
        self.models = models
        self.exclude_models = exclude_models
        self.selector = selector
        self.data_tests = data_tests
        self.schema_tests = schema_tests

    def shell_command(self):
        command = super().shell_command()
        models = ' '.join(self.models) if self.models else None
        exclude_models = ' '.join(self.exclude_models) if self.exclude_models else None
        command += (' -x'
                   + (f' --models {models}' if self.models else '')
                   + (f' --exclude {exclude_models}' if self.exclude_models else '')
                   + (' --data' if self.data_tests else '')
                   + (' --schema' if self.data_tests else '')
                   + (f' --selector {self.selector}' if self.selector else ''))
        return command

    def html_doc_items(self) -> [(str, str)]:
        return [
            ('db', _.tt[self.db_alias]),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('data tests', _.tt[self.data_tests]),
            ('schema tests', _.tt[self.schema_tests]),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None)
        ]
