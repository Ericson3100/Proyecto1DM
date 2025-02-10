from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from mage_ai.io.snowflake import Snowflake
from os import path
from pandas import DataFrame


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(data_frames: dict, **kwargs) -> None:
    """
    Export multiple tables' data to a Snowflake warehouse.
    Specify your configuration settings in 'io_config.yaml'.
    """
    database = 'INSTACART_DB'
    schema = 'RAW'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Asegúrate de que los nombres de las tablas estén en minúsculas sin comillas.
    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table_name, df in data_frames.items():
            loader.export(
                df,
                table_name,  # Nombre de la tabla en minúsculas sin comillas
                database,
                schema,
                if_exists='replace',  # Especificar la política de resolución si la tabla ya existe
            )
