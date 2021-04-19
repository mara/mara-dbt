import re

from setuptools import setup, find_packages


def get_long_description():
    with open('README.md') as f:
        return re.sub('!\[(.*?)\]\(docs/(.*?)\)',
                      r'![\1](https://github.com/mara/mara-dbt/raw/master/docs/\2)', f.read())

dbt_package_version = "0.19.0"

setup(
    name='mara-dbt',
    version='0.1.0',

    description='Lightweight dbt integration into the mara framework',

    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    url='https://github.com/mara/mara-dbt',

    install_requires=[
        'mara-pipelines>=3.1.0',
        'mara-db>=4.7.1',
        'mara-page>=1.3.0',
        f'dbt-core=={dbt_package_version}',
        'PyYAML>=5.4.1'],

    setup_requires=['setuptools_scm'],
    include_package_data=True,

    extras_require={
        'postgres': [f'dbt-postgres=={dbt_package_version}'],
        'redshift': [f'dbt-redshift=={dbt_package_version}'],
        'bigquery': [f'dbt-bigquery=={dbt_package_version}'],
        'sqlserver': [f'dbt-sqlserver~={dbt_package_version}'],
        'test': ['pytest', 'pytest_click'],
    },

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    python_requires='>=3.6.2'
)
