import json
import shlex
from warnings import warn

from mara_page import _
from mara_pipelines.pipelines import Command

from . import config


class _DbtCommand(Command):
    def __init__(self, command: str, target: str = None, variables: dict = None):
        """
        Executes a dbt command

        Args
            command: the dbt command
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__()
        self._dbt_command = command
        self.variables = variables
        self.target = target or config.dbt_target()

    def shell_command(self):
        variables = dict(config.dbt_variables() or {})
        if self.variables:
            variables.update(self.variables)

        return (f'dbt --no-use-colors {self._dbt_command}'
                + (f' --project-dir {config.project_dir()}' if config.project_dir() else '')
                + (f' --profiles-dir {config.profiles_dir()}' if config.profiles_dir() else '')
                + (f' --profile {config.profile()}' if config.profile() else '')
                + (f' -t {self.target}' if self.target else '')
                + (f' --vars {shlex.quote(str(variables))}' if variables else ''))


class DbtSeed(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
                 selector: str = None, full_refresh: bool = False,
                 target: str = None, variables: dict = None):
        """
        Executes dbt seed

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: Drop existing seed tables and recreate them
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('seed', target=target, variables=variables)
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
            ('target', _.tt[self.target] if self.target else None),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None),
            ('full refresh', _.tt[self.full_refresh])
        ]


class DbtSnapshot(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
        selector: str = None, target: str = None, variables: dict = None):
        """
        Executes dbt snapshot

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('run', target=target, variables=variables)
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
            ('target', _.tt[self.target] if self.target else None),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None)
        ]


class DbtRun(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
        selector: str = None, full_refresh: bool = False,
        target: str = None, variables: dict = None):
        """
        Executes dbt run

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: If specified, DBT will drop incremental models and
                          fully-recalculate the incremental table from the model
                          definition.
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('run', target=target, variables=variables)
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
            ('target', _.tt[self.target] if self.target else None),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None),
            ('full refresh', _.tt[self.full_refresh])
        ]


class DbtTest(_DbtCommand):
    def __init__(self, models: [str] = None, exclude_models: [str] = None,
        selector: str = None, data_tests: bool = False, schema_tests: bool = False,
        target: str = None, variables: dict = None):
        """
        Executes dbt test

        Args
            models: Specify the nodes to include.
            exclude_models: Specify the models to exclude.
            selector: The selector name to use, as defined in selectors.yml
            data_tests: Run data tests defined in "tests" directory.
            schema_tests: Run constraint validations from schema.yml files
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('test', target=target, variables=variables)
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
            ('target', _.tt[self.target] if self.target else None),
            ('include models', _.tt[self.models] if self.models else None),
            ('exclude models', _.tt[self.exclude_models] if self.exclude_models else None),
            ('data tests', _.tt[self.data_tests]),
            ('schema tests', _.tt[self.schema_tests]),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None)
        ]



class _DbtCloudCommand(Command):
    def __init__(self):
        """
        Executes a dbt command against the cloud
        """
        super().__init__()

    def shell_command(self):
        return (
                (f'DBT_CLOUD_HOST={config.dbt_cloud_host()} ' if config.dbt_cloud_host() else '') +
                (f'DBT_CLOUD_API_TOKEN={config.dbt_cloud_api_token()} ' if config.dbt_cloud_api_token() else '') +
                (f'DBT_CLOUD_ACCOUNT_ID={config.dbt_cloud_account_id()} ' if config.dbt_cloud_account_id() else '') +
                'dbt-cloud')


class RunDbtCloudJob(_DbtCloudCommand):
    def __init__(self, job_id: int, cause: str = None, wait: bool = True):
        
        """
        Starts a dbt cloud job.

        Args:
            job_id: The job id of the cloud job
            cause: The cause text send during execution
            wait: If the command waits until the job is finished.
        """
        super().__init__()
        self.job_id = job_id
        self.cause = cause
        self.wait = wait
    
    def shell_command(self):
        return (super().shell_command()
            + f' job run --job-id {self.job_id}'
            + (f' --cause {shlex.quote(self.cause)}' if self.cause else '')
            + (' --wait' if self.wait else ''))


# deprecated. TBD: Remove in 1.0.0
class RunDbtJob(RunDbtCloudJob):
    def __init_subclass__(cls) -> None:
        warn("Class RunDbtJob has been renamed to RunDbtCloudJob", DeprecationWarning, stacklevel=2)
        return super().__init_subclass__()
