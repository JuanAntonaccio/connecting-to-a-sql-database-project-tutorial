import pandas as pd 
import numpy as np 
import sqlalchemy as db 
import pymysql
import os
import psycopg2
from dotenv import load_dotenv 
from sqlalchemy import create_engine
#from sqlalchemy import *


load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT =  os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
#DB_CORE = os.getenv('DB_CORE')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_NAME= os.getenv("DB_NAME")
print("Pase por aca.... primer bloque")



conn = psycopg2.connect(database=DB_DATABASE,
                        user=DB_USER,
                        password=DB_PASSWORD,
                        host=DB_HOST,
                        port=DB_PORT)





# 1) Connect to the database here using the SQLAlchemy's create_engine function
# Utilice otro metodo visto en la clase con una base de datos postgre

cursor = conn.cursor()






# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
def tabla1():
    creaPubl="""
        CREATE TABLE publishers(
        publisher_id INT NOT NULL,
        name VARCHAR(255) NOT NULL,
        PRIMARY KEY(publisher_id)
        );
    """
    cursor.execute(creaPubl)
    conn.commit()

def tabla2():
    creaAut="""
        CREATE TABLE authors(
        author_id INT NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        middle_name VARCHAR(50) NULL,
        last_name VARCHAR(100) NULL,
        PRIMARY KEY(author_id)
        );
    """
    cursor.execute(creaAut)
    conn.commit()
def tabla3():
    creaBok="""
        CREATE TABLE books(
        book_id INT NOT NULL,
        title VARCHAR(255) NOT NULL,
        total_pages INT NULL,
        rating DECIMAL(4, 2) NULL,
        isbn VARCHAR(13) NULL,
        published_date DATE,
        publisher_id INT NULL,
        PRIMARY KEY(book_id),
        CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
    );
    """
    cursor.execute(creaBok)
    conn.commit()

tabla1()
tabla2()
tabla3()
conn.close()






# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function



# 4) Use pandas to print one of the tables as dataframes using read_sql function
