from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_multiple_tables(*args, **kwargs):
    """
    Load data from multiple tables in a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.
    """

    queries = {
        'AISLES': 'SELECT * FROM aisles',  
        'DEPARTMENTS': 'SELECT * FROM departments',
        'PRODUCTS': 'SELECT * FROM products',  
        'ORDER_PRODUCTS': 'SELECT * FROM order_products',
        'INSTACART_ORDERS': 'SELECT * FROM instacart_orders',      

    }
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')

    config_profile = 'default'
    
    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
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

