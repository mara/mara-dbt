import json
import shlex
from warnings import warn
from typing import Optional, List, Tuple, Union

from mara_page import _
from mara_pipelines.pipelines import Command

from . import config


class _DbtCommand(Command):
    """ A base class for a dbt cli command """
    def __init__(self, command: str, target: Optional[str] = None, variables: Optional[dict] = None):
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

        return (f'dbt -x --no-use-colors {self._dbt_command}'
                + (f' --project-dir {config.project_dir()}' if config.project_dir() else '')
                + (f' --profiles-dir {config.profiles_dir()}' if config.profiles_dir() else '')
                + (f' --profile {config.profile()}' if config.profile() else '')
                + (f' -t {self.target}' if self.target else '')
                + (f' --vars {shlex.quote(str(variables))}' if variables else ''))

    def html_doc_items(self) -> List[Tuple[str, str]]:
        return [
            ('target', _.tt[self.target] if self.target else None),
            ('variables', _.tt[json.dump(self.variables)] if self.variables else None),
        ]


class _DbtSelectCommand(_DbtCommand):
    """ A base class for a dbt cli command which supports selecting nodes """
    def __init__(self, command: str, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
                 selector: Optional[str] = None, full_refresh: Optional[bool] = None, target: Optional[str] = None, variables: Optional[dict] = None):
        """
        Executes a dbt command

        Args
            command: the dbt command
            select: Specify the nodes to include.
            exclude_models: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: If specified, dbt will drop seeds and/or incremental models and fully-recalculate the models from their definition.
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__(command, target, variables)
        self.select = select
        self.exclude = exclude
        self.selector = selector
        self.full_refresh = full_refresh

    def shell_command(self):
        command = super().shell_command()
        selects = ' '.join(self.select) if isinstance(self.select, list) else self.select
        excludes = ' '.join(self.exclude) if isinstance(self.exclude, list) else self.exclude
        command += ((f' -s {selects}' if selects else '')
                   + (f' --exclude {excludes}' if excludes else '')
                   + (f' --selector {self.selector}' if self.selector else '')
                   + (f' --full-refresh' if self.full_refresh else ''))
        return command

    def html_doc_items(self) -> List[Tuple[str, str]]:
        return super().html_doc_items() + [
            ('include nodes', _.tt[self.select] if self.select else None),
            ('exclude nodes', _.tt[self.exclude] if self.exclude else None),
            ('selector', _.tt[self.selector] if self.selector else None),
            ('full refresh', _.tt[self.full_refresh] if self.full_refresh is not None else None),
        ]


class DbtDocsGenerate(_DbtSelectCommand):
    def __init__(self, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
                 selector: Optional[str] = None, no_compile: bool = False,
                 target: Optional[str] = None, variables: Optional[dict] = None) -> None:
        """
        Executes dbt docs generate

        Args
            select: Specify the nodes to include.
            exclude: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            no_compile: Do not run "dbt compile" as part of docs generation
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('docs generate', select=select, exclude=exclude, selector=selector,
                         target=target, variables=variables)
        self.no_compile = no_compile

    def shell_command(self):
        command = super().shell_command()
        command += (' --no-compile' if self.no_compile else '')
        return command

    def html_doc_items(self) -> List[Tuple[str, str]]:
        return super().html_doc_items() + [
            ('no compile', self.no_compile),
        ]


class DbtDeps(_DbtCommand):
    def __init__(self, target: Optional[str] = None, variables: Optional[dict] = None):
        """
        Executes dbt deps

        Args
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('deps', target=target, variables=variables)


class DbtSeed(_DbtSelectCommand):
    def __init__(self, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
                 selector: Optional[str] = None, full_refresh: bool = False,
                 target: Optional[str] = None, variables: Optional[dict] = None, **kargs):
        """
        Executes dbt seed

        Args
            select: Specify the nodes to include.
            exclude: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: Drop existing seed tables and recreate them
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        if select is None and 'models' in kargs:
            warn("Use parameter 'select' instead of 'models' in command DbtRun", DeprecationWarning, stacklevel=2)
            select = kargs['model']
        if exclude is None and 'exclude_models' in kargs:
            warn("Use parameter 'exclude' instead of 'exclude_models' command DbtRun", DeprecationWarning, stacklevel=2)
            exclude = kargs['exclude_models']
        super().__init__('seed', select=select, exclude=exclude, selector=selector, full_refresh=full_refresh,
                         target=target, variables=variables)


class DbtBuild(_DbtSelectCommand):
    def __init__(self, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
        selector: Optional[str] = None, full_refresh: bool = False,
        target: Optional[str] = None, variables: Optional[dict] = None):
        """
        Executes dbt build

        Args
            select: Specify the nodes to include.
            exclude: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: If specified, dbt will drop incremental models and
                          fully-recalculate the incremental table from the model
                          definition.
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('build', select=select, exclude=exclude, selector=selector, full_refresh=full_refresh,
                         target=target, variables=variables)


class DbtSnapshot(_DbtSelectCommand):
    def __init__(self, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
        selector: Optional[str] = None, target: Optional[str] = None, variables: Optional[dict] = None, **kargs):
        """
        Executes dbt snapshot

        Args
            select: Specify the nodes to include.
            exclude: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        if select is None and 'models' in kargs:
            warn("Use parameter 'select' instead of 'models' in command DbtRun", DeprecationWarning, stacklevel=2)
            select = kargs['model']
        if exclude is None and 'exclude_models' in kargs:
            warn("Use parameter 'exclude' instead of 'exclude_models' command DbtRun", DeprecationWarning, stacklevel=2)
            exclude = kargs['exclude_models']
        super().__init__('run', select=select, exclude=exclude, selector=selector,
                         target=target, variables=variables)


class DbtRun(_DbtSelectCommand):
    def __init__(self, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
        selector: Optional[str] = None, full_refresh: bool = False,
        target: Optional[str] = None, variables: Optional[dict] = None, **kargs):
        """
        Executes dbt run

        Args
            select: Specify the nodes to include.
            exclude: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: If specified, DBT will drop incremental models and
                          fully-recalculate the incremental table from the model
                          definition.
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        if select is None and 'models' in kargs:
            warn("Use parameter 'select' instead of 'models' in command DbtRun", DeprecationWarning, stacklevel=2)
            select = kargs['model']
        if exclude is None and 'exclude_models' in kargs:
            warn("Use parameter 'exclude' instead of 'exclude_models' command DbtRun", DeprecationWarning, stacklevel=2)
            exclude = kargs['exclude_models']
        super().__init__('run', select=select, exclude=exclude, selector=selector, full_refresh=full_refresh,
                         target=target, variables=variables)


class DbtCompile(_DbtSelectCommand):
    def __init__(self, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
        selector: Optional[str] = None, full_refresh: bool = False,
        target: Optional[str] = None, variables: Optional[dict] = None):
        """
        Executes dbt compile

        Args
            select: Specify the nodes to include.
            exclude: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            full_refresh: If specified, DBT will drop incremental models and
                          fully-recalculate the incremental table from the model
                          definition.
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        super().__init__('compile', select=select, exclude=exclude, selector=selector, full_refresh=full_refresh,
                         target=target, variables=variables)


class DbtTest(_DbtSelectCommand):
    def __init__(self, select: Optional[Union[List[str], str]] = None, exclude: Optional[Union[List[str], str]] = None,
        selector: Optional[str] = None, data_tests: bool = False, schema_tests: bool = False,
        target: Optional[str] = None, variables: Optional[dict] = None, **kargs):
        """
        Executes dbt test

        Args
            select: Specify the nodes to include.
            exclude: Specify the nodes to exclude.
            selector: The selector name to use, as defined in selectors.yml
            data_tests: Run data tests defined in "tests" directory.
            schema_tests: Run constraint validations from schema.yml files
            target: the dbt target. If not set config.dbt_target() is used.
            variables: Supply variables to the project. This argument
                       overrides variables defined in config.dbt_variables()
        """
        if select is None and 'models' in kargs:
            warn("Use parameter 'select' instead of 'models' in command DbtRun", DeprecationWarning, stacklevel=2)
            select = kargs['model']
        if exclude is None and 'exclude_models' in kargs:
            warn("Use parameter 'exclude' instead of 'exclude_models' command DbtRun", DeprecationWarning, stacklevel=2)
            exclude = kargs['exclude_models']
        super().__init__('test', select=select, exclude=exclude, selector=selector,
                         target=target, variables=variables)
        self.data_tests = data_tests
        self.schema_tests = schema_tests

    def shell_command(self):
        command = super().shell_command()
        command += ((' --data' if self.data_tests else '')
                   + (' --schema' if self.data_tests else ''))
        return command

    def html_doc_items(self) -> List[Tuple[str, str]]:
        return super().html_doc_items() + [
            ('data tests', _.tt[self.data_tests]),
            ('schema tests', _.tt[self.schema_tests]),
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
    def __init__(self, job_id: int, cause: Optional[str] = None, wait: bool = True):

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
