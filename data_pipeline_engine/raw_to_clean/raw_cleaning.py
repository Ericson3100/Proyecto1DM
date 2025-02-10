from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
import pandas as pd
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def execute_transformer_action(data_frames: dict, *args, **kwargs) -> dict:
    """
    Transform data based on table-specific rules.
    """
    if 'PRODUCTS' in data_frames:
        df = data_frames['PRODUCTS']
        df['PRODUCT_NAME'].fillna('Unknown Product', inplace=True)
    
    if 'ORDER_PRODUCTS' in data_frames:
        df = data_frames['ORDER_PRODUCTS']
        df['ADD_TO_CART_ORDER'].fillna(1, inplace=True)
        df['ADD_TO_CART_ORDER'] = df['ADD_TO_CART_ORDER'].astype(int)
        df['REORDERED'] = df['REORDERED'].astype(bool)
    
    if 'INSTACART_ORDERS' in data_frames:
        df = data_frames['INSTACART_ORDERS']
        df['DAYS_SINCE_PRIOR_ORDER'].fillna(-1, inplace=True)
        df['DAYS_SINCE_PRIOR_ORDER'] = df['DAYS_SINCE_PRIOR_ORDER'].astype(int)
        df.drop(columns=['ID'], errors='ignore', inplace=True)  # Elimina la columna 'id' si existe
        df.drop_duplicates(inplace=True)
        df['FECHA'] = pd.to_datetime('today').strftime('%Y-%m-%d')  # Añade la columna de fecha con la fecha actual
    
    return data_frames

@test
def test_output(output, *args) -> None:
    """
    Validations for transformed data.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'Output should be a dictionary'
    if 'INSTACART_ORDERS' in output:
        assert 'FECHA' in output['INSTACART_ORDERS'].columns, "La columna 'fecha' no fue añadida correctamente."
