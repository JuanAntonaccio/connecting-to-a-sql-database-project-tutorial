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
#DB_ = os.getenv('DB_DATABASE')
DB_NAME= os.getenv("DB_NAME")
print("Pase por aca.... primer bloque")



conn = psycopg2.connect(database=DB_NAME,
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

def tabla4():
    creaBoAu="""
        CREATE TABLE book_authors (
        book_id INT NOT NULL,
        author_id INT NOT NULL,
        PRIMARY KEY(book_id, author_id),
        CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE,
        CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE
        );
    """
    cursor.execute(creaBoAu)
    conn.commit()    

#tabla1()
#tabla2()
#tabla3()
#tabla4()
print("#"*50)
print("Se han creado las 4 tablas en forma correcta. ok")
print("#"*50)







# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function
insert_publi="""
            INSERT INTO publishers(publisher_id,name) values (1,'O Reilly Media');
            INSERT INTO publishers(publisher_id,name) values (2,'A Book Apart');
            INSERT INTO publishers(publisher_id,name) values (3,'A K PETERS');
            INSERT INTO publishers(publisher_id,name) values (4,'Academic Press');
            INSERT INTO publishers(publisher_id,name) values (5,'Addison Wesley');
            INSERT INTO publishers(publisher_id,name) values (6,'Albert&Sweigart');
            INSERT INTO publishers(publisher_id,name) values (7,'Alfred A. Knopf');
"""
#cursor.execute(insert_publi)
#conn.commit()
print("Se han insertado bien en la tabla publishers")
insert_author="""
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (1,'Merritt',null,'Eric');
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (2,'Linda',null,'Mui');
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (3,'Alecos',null,'Papadatos');
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (4,'Anthony',null,'Molinaro');
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (5,'David',null,'Cronin');
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (6,'Richard',null,'Blum');
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (7,'Yuval','Noah','Harari');
        INSERT INTO authors(author_id,first_name,middle_name,last_name) values (8,'Paul',null,'Albitz');
"""
#cursor.execute(insert_author)
#conn.commit()
#conn.close()
print("Se han insertado bien en la tabla authors")

insert_book=""" 
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (1,'Lean Software Development: An Agile Toolkit',240,4.17,'9780320000000','2003-05-18',5);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (2,'Facing the Intelligence Explosion',91,3.87,null,'2013-02-01',7);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (3,'Scala in Action',419,3.74,'9781940000000','2013-04-10',1);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (4,'Patterns of Software: Tales from the Software Community',256,3.84,'9780200000000','1996-08-15',1);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (5,'Anatomy Of LISP',446,4.43,'9780070000000','1978-01-01',3);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (6,'Computing machinery and intelligence',24,4.17,null,'2009-03-22',4);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (7,'XML: Visual QuickStart Guide',269,3.66,'9780320000000','2009-01-01',5);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (8,'SQL Cookbook',595,3.95,'9780600000000','2005-12-01',7);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (9,'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)',439,4.29,'9781440000000','2010-07-01',6);
        insert into books (book_id,title,total_pages,rating,isbn,published_date,publisher_id) values (10,'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence',222,3.54,'9780750000000','2007-02-13',7);
"""

#cursor.execute(insert_book)
#conn.commit()
#conn.close()
print("Se han insertado bien en la tabla books")

insert_boAu="""
       INSERT INTO book_authors (book_id, author_id) values (8,4);
       
"""

#cursor.execute(insert_boAu)
#conn.commit()
#conn.close()
print("Se han insertado bien en la tabla autor books")





# 4) Use pandas to print one of the tables as dataframes using read_sql function

df = pd.read_sql("SELECT * FROM books;",conn)
print(df)

conn.close()


