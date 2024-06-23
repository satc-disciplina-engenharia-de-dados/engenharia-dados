'''This module is used to connect to a postgres db and insert data into it and collect data from it'''
import psycopg2 as pg
from psycopg2 import sql
from psycopg2.extras import execute_values

class Postgres():
    '''CLASS TO HANDLE POSTGRES DB CONNECTIONS AND INSERTIONS

    Attributes
    ----------
    __url : urllib.parse.ParseResult
        URL to connect to the postgres db
    __db : str
        Database to be used
    __table : str
        Table to be used
    username : str
        Username to connect to the postgres db
    password : str
        Password to connect to the postgres db
    host : str
        Host to connect to the postgres db
    port : str
        Port to connect to the postgres db
    cursor : psycopg2.extensions.cursor
        Cursor to interact with the postgres db
    '''

    def __init__(self, user, password, host, port, db):
        self.__db = db
        self.__table = ''
        self.username = user
        self.password = password
        self.host = host
        self.port = port
        self.cursor = ''
        self.connection = ''

    def set_db(self, db):
        '''Set the database to be used'''
        self.__db = db

    def set_table(self, table):
        '''Set the table to be used'''
        self.__table = table

    def connect(self):
        '''Connect to the postgres db'''
        try:
            connection = pg.connect(
                dbname=self.__db,
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = connection.cursor()
            return self.cursor
        except Exception as e:
            raise e
        
    def get_all_tables(self):
        '''Get all tables in the database'''
        query = "SELECT table_name \
                FROM information_schema.tables \
                WHERE table_schema='public' AND table_type='public'"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_all_data(self, columns : str = ''):
        '''Get all data from the table'''
        if columns == '':
            query = f"SELECT * FROM {self.__table}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        query = f"SELECT {columns} FROM {self.__table}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_data_with_condition(self, condition : str, columns : str = ''):
        '''Get data from the table with a condition'''
        if columns == '':
            query = f"SELECT * FROM {self.__table} WHERE {condition}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        query = f"SELECT {columns} FROM {self.__table} WHERE {condition}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_data(self, data : dict):
        '''Insert data into the table'''
        columns = ', '.join(data.keys())
        values = ', '.join([f"'{value}'" for value in data.values()])
        query = f"INSERT INTO {self.__table} ({columns}) VALUES ({values})"
        self.cursor.execute(query)
        self.cursor.connection.commit()

    def insert_many_data(self, collumns: str, data_tuples: list):
        '''Insert many data into the table'''
        insert_query = sql.SQL(f"INSERT INTO {self.__table} ({collumns}) VALUES " + "%s")
        execute_values(self.cursor, insert_query, data_tuples)
        self.cursor.connection.commit()