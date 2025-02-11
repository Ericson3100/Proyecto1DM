from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake 
from os import path


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_multiple_tables(*args, **kwargs):
    """
    Load data from multiple tables in a Snowflake database.
    Specify your configuration settings in 'io_config.yaml'.
    """

    # Define the schema and database as variables
    db_schema = 'RAW'  
    db_database = 'INSTACART_DB'  
    # Consultas SQL con esquema y base de datos
    queries = {
        'AISLES': f'SELECT * FROM {db_database}.{db_schema}.aisles',  
        'DEPARTMENTS': f'SELECT * FROM {db_database}.{db_schema}.departments',
        'PRODUCTS': f'SELECT * FROM {db_database}.{db_schema}.products',  
        'ORDER_PRODUCTS': f'SELECT * FROM {db_database}.{db_schema}.order_products',
        'INSTACART_ORDERS': f'SELECT * FROM {db_database}.{db_schema}.instacart_orders',      
    }
    
    # Ruta de configuración y perfil
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    # Cargando datos usando la configuración y las consultas con Snowflake
    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        data_frames = {table_name: loader.load(query) for table_name, query in queries.items()}
    
    return data_frames


@test
def test_output(output, *args) -> None:
    """
    Test that the output is not empty and contains data for all tables.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'Output should be a dictionary'
    assert all(not df.empty for df in output.values()), 'One or more tables returned empty results'
