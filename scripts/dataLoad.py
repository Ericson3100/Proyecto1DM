import os
from mysql.connector import errorcode
import csv
import mysql.connector
import json
# Function to load the status of the data load from a file json
def load_status():
    if not os.path.exists('data/actual_row.json'):
        with open('data/actual_row.json', 'w') as file:
            actual_row = {
                'aisles': 0,
                'departments': 0,
                'products': 0,
                'order_products': 0,
                'instacart_orders': 0
            }
            json.dump(actual_row, file)
    else:
        with open('data/actual_row.json', 'r') as file:
            actual_row = json.load(file) 
    return actual_row

def reset_status():
    with open('data/actual_row.json', 'w') as file:
        actual_row = {
            'aisles': 0,
            'departments': 0,
            'products': 0,
            'order_products': 0,
            'instacart_orders': 0
        }
        json.dump(actual_row, file)    

import json
import csv
import mysql.connector

# Function to load data from CSV file into a table
def load_data_from_csv(file_path, table_name, cursor, cnx, status, json_file='data/actual_row.json',duplicate=False):
    with open(file_path, mode='r', encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file, delimiter=';')
        headers = next(csv_reader)  # Skip header row
        headers = ', '.join(headers)
        if(duplicate):
            headers= 'id, '+headers
        # Skip rows if necessary
        if status[table_name] > 0:
            for i in range(status[table_name]):
                next(csv_reader)
        cont=0
        for row in csv_reader:
            row = [None if value == '' else value for value in row]  # Avoid empty string data errors
            cont+=1
            if(duplicate):
                value=status[table_name]+cont
                row=[str(value)]+row
            
            placeholders = ', '.join(['%s'] * len(row))
            
            sql = f"INSERT INTO {table_name} ({headers}) VALUES ({placeholders})"

            try:
                cursor.execute(sql, row)  # Try to execute the SQL query
                
                if(cont==10000):
                    cnx.commit()      
                    status[table_name] += 10000  # Update status if the insert is successful
                    # Update JSON file with the current status
                    with open(json_file, 'w') as file:
                        json.dump(status, file)  
                    cont=0
            except mysql.connector.IntegrityError as e:
                # Handle duplicate index or other integrity errors
                print(f"Error with row {row}: {e}. Skipping to next row.")
                # Update JSON file with the next row to load
                status[table_name] += 10000  # Update status if the insert is successful
                with open(json_file, 'w') as file:
                    json.dump(status, file)
                continue  # Skip this row and continue with the next one

            except Exception as e:
                # Handle any other errors
                print(f"Unexpected error with row {row}: {e}. Skipping to next row.")
                continue  # Skip this row and continue with the next one
        cnx.commit()
        status[table_name] +=cont  
        with open(json_file, 'w') as file:
            json.dump(status, file)
        print(f"Data loaded into {table_name} table successfully.")


#Info updated constantly in a file to save the progress of the data load and just load the data that is missing
status = load_status()


# Database connection configuration using environment variables
config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'raise_on_warnings': True
}


# Inicializar variables
cnx = None
cursor = None
tablaAisles='''CREATE TABLE IF NOT EXISTS AISLES (
    AISLE_ID INT,
    AISLE VARCHAR(255) NOT NULL,
    PRIMARY KEY (AISLE_ID))'''
tablaDepartments='''CREATE TABLE IF NOT EXISTS DEPARTMENTS (
    DEPARTMENT_ID INT,
    DEPARTMENT VARCHAR(255) NOT NULL,
    PRIMARY KEY (DEPARTMENT_ID))'''
tablaProducts='''CREATE TABLE IF NOT EXISTS PRODUCTS (
    PRODUCT_ID INT,
    PRODUCT_NAME VARCHAR(255),
    AISLE_ID INT, 
    DEPARTMENT_ID INT, 
    PRIMARY KEY (PRODUCT_ID),
    FOREIGN KEY (AISLE_ID) REFERENCES AISLES(AISLE_ID),
    FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS(DEPARTMENT_ID))
    '''
tablaInstacartOrders='''CREATE TABLE IF NOT EXISTS INSTACART_ORDERS (
    ID INT PRIMARY KEY,
    ORDER_ID INT,
    USER_ID INT,
    ORDER_NUMBER INT,
    ORDER_DOW INT,
    ORDER_HOUR_OF_DAY INT,
    DAYS_SINCE_PRIOR_ORDER FLOAT)'''
tablaOrderProducts='''CREATE TABLE IF NOT EXISTS ORDER_PRODUCTS (
    ORDER_ID INT, 
    PRODUCT_ID INT, 
    ADD_TO_CART_ORDER FLOAT,
    REORDERED INT,
    PRIMARY KEY (ORDER_ID, PRODUCT_ID),
    FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCTS(PRODUCT_ID))
    '''

    
try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    #Ingreso si desea crear todo desde 0 o sobreescribir las configuraciones actuales de la base de datos
    print("Do you want to create the database and tables from scratch or overwrite the current database configuration?")
    print("1. Create from scratch")
    print("2. Overwrite current configuration")
    print("3. Continue fetching data since last checkpoint")
    while True:
        opcion = input("Enter the number of the option you want: ")
        if opcion == "1":
            reset_status()
            status = load_status()
            cursor.execute("DROP DATABASE IF EXISTS instacart_db")
            # Create database
            cursor.execute("CREATE DATABASE IF NOT EXISTS instacart_db")
            cursor.execute("USE instacart_db")

            # Create tables
            cursor.execute(tablaAisles)
            cursor.execute(tablaDepartments)
            cursor.execute(tablaProducts)
            cursor.execute(tablaInstacartOrders)
            cursor.execute(tablaOrderProducts)
            
            # Commit the transaction
            cnx.commit()
            print("Database and tables created successfully.")
            break
        elif opcion == "2":
            reset_status()
            cursor.execute("USE instacart_db")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("TRUNCATE TABLE AISLES")
            cursor.execute("TRUNCATE TABLE DEPARTMENTS")
            cursor.execute("TRUNCATE TABLE PRODUCTS")
            cursor.execute("TRUNCATE TABLE ORDER_PRODUCTS")
            cursor.execute("TRUNCATE TABLE INSTACART_ORDERS")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")#Esto es para que no haya problemas con las llaves foráneas
            cnx.commit()
            break
        elif opcion == "3":
            cursor.execute("USE instacart_db")
            break
        else:
            print("Opción no válida")


 
    # Cargar datos
    print("Loading data into tables...")

    # Load data into tables
    load_data_from_csv('data/aisles.csv', 'aisles', cursor,cnx,status)
    load_data_from_csv('data/departments.csv', 'departments', cursor,cnx,status)
    load_data_from_csv('data/products.csv', 'products', cursor,cnx,status)
    load_data_from_csv('data/instacart_orders.csv', 'instacart_orders', cursor,cnx,status,duplicate=True)
    load_data_from_csv('data/order_products.csv', 'order_products', cursor,cnx,status)



except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
finally:
    # Cerrar cursor y conexión si están definidos
    if cursor:
        cursor.close()
    if cnx:
        cnx.close()
