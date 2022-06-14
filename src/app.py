import pandas as pd 
import numpy as np 
import sqlalchemy as db 
import os
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

def connect():
    global engine # this allows us to use a global variable called engine
    # A "connection string" is basically a string that contains all the databse credentials together
    connection_string = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}?autocommit=true"
    print("Starting the connection...")
    engine = create_engine(connection_string)
    engine.connect()
    return engine



# 1) Connect to the database here using the SQLAlchemy's create_engine function

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

# 4) Use pandas to print one of the tables as dataframes using read_sql function
