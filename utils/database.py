import pandas as pd
import mysql.connector
from dotenv import dotenv_values


config = dotenv_values('.env')

# Connect to the database.
def db_connection() -> object:
    try:
        host = config.get('host')
        user = config.get('user')
        password = config.get('password')
        database = config.get('database')
    
        db = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = db.cursor()
        print('\nConnected to MySQL database!\n')     
        return db, cursor
    except Exception as e:
        print(f"Error: {e}")
        cursor.close()
        db.close()        

# Save the scraped data to the database
def save_to_mysql(db:object, cursor:object, data:pd.DataFrame):
    try:
        # Create the table
        table_name = 'premier_league_table'
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            No INT PRIMARY KEY,
            Teams VARCHAR(50),
            GP INT,
            W INT,
            D INT,
            L INT,
            GF INT,
            GA INT,
            GD INT,
            PTS INT
        )
        ''')

        data_to_insert = [tuple(row) for index, row in data.iterrows()]
        
        query = f'''
            INSERT INTO {table_name} 
            (No, Teams, GP, W, D, L, GF, GA, GD, PTS) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Teams = VALUES(Teams),
            GP = VALUES(GP),
            W = VALUES(W),
            D = VALUES(D),
            L = VALUES(L),
            GF = VALUES(GF),
            GA = VALUES(GA),
            GD = VALUES(GD),
            PTS = VALUES(PTS)
        '''
        
        # Insert data into the table
        cursor.executemany(query, data_to_insert)
        db.commit()

        print('Data inserted successfully')
    except Exception as e:
        print(f'Error: {e}')
        db.rollback()