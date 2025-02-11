from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def execute_transformer_action(data_frames: dict, *args, **kwargs) -> dict:
    """
    Transform data based on table-specific rules and create the new dataframe
    with DIM_PRODUCTO, FCT_ORDERS, and FCT_ORDERS_PRODUCTO.
    """
    
    # Cargar las tablas de entrada
    df_products = data_frames.get('PRODUCTS', pd.DataFrame())
    df_aisles = data_frames.get('AISLES', pd.DataFrame())
    df_departments = data_frames.get('DEPARTMENTS', pd.DataFrame())
    df_instacart_orders = data_frames.get('INSTACART_ORDERS', pd.DataFrame())
    df_order_products = data_frames.get('ORDER_PRODUCTS', pd.DataFrame())
        
    # Crear la tabla DIM_PRODUCTO:
    df_dim_producto = df_products.copy()
    
    # Merge con AISLES y DEPARTMENTS para obtener los nombres
    df_dim_producto = df_dim_producto.merge(df_aisles[['AISLE_ID', 'AISLE']], on='AISLE_ID', how='left')
    df_dim_producto = df_dim_producto.merge(df_departments[['DEPARTMENT_ID', 'DEPARTMENT']], on='DEPARTMENT_ID', how='left')

    # Eliminar las columnas AISLE_ID y DEPARTMENT_ID
    df_dim_producto.drop(columns=['AISLE_ID', 'DEPARTMENT_ID'], inplace=True)

    # Crear la tabla FCT_ORDERS (misma que INSTACART_ORDERS):
    df_fct_orders = df_instacart_orders.copy()
    df_fct_orders['FECHA'] = pd.to_datetime('today').strftime('%Y-%m-%d')  # Añadir la columna FECHA
    
    # Crear la tabla FCT_ORDERS_PRODUCTO (misma que ORDER_PRODUCTS):
    df_fct_orders_producto = df_order_products.copy()
    
    # Crear el diccionario final de dataframes:
    new_data_frames = {
        'DIM_PRODUCTO': df_dim_producto,
        'FCT_ORDERS': df_fct_orders,
        'FCT_ORDERS_PRODUCTO': df_fct_orders_producto
    }
    
    return new_data_frames

@test
def test_output(output, *args) -> None:
    """
    Validations for transformed data.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, dict), 'Output should be a dictionary'
    
    # Validaciones para DIM_PRODUCTO
    if 'DIM_PRODUCTO' in output:
        assert 'AISLE' in output['DIM_PRODUCTO'].columns, "La columna 'AISLE' no fue añadida correctamente."
        assert 'DEPARTMENT' in output['DIM_PRODUCTO'].columns, "La columna 'DEPARTMENT' no fue añadida correctamente."
    
    # Validaciones para FCT_ORDERS
    if 'FCT_ORDERS' in output:
        assert 'FECHA' in output['FCT_ORDERS'].columns, "La columna 'FECHA' no fue añadida correctamente."
    
    # Validaciones para FCT_ORDERS_PRODUCTO
    if 'FCT_ORDERS_PRODUCTO' in output:
        assert 'ADD_TO_CART_ORDER' in output['FCT_ORDERS_PRODUCTO'].columns, "La columna 'ADD_TO_CART_ORDER' no fue añadida correctamente."
