from multiprocessing import connection
from operator import index
from select import select
import pandas as pd
import sqlite3

class DatabaseManagement():
    
    def __init__(self, dataframe, database:str, table:str):
        """Class to handle database-connection and queries in sqlite3.

        Args:
            dataframe ([type]): Dataframe that will be connected to database.
            database (str): Name of new database.
            table (str): Name of first sql-table created in database.
        """
        self.df = dataframe
        self.database = database
        self.table = table
        
    def create_database(self):
        """Method to create new database
        """
        self.conn = sqlite3.connect(self.database)
        self.cur = self.conn.cursor()
              
    def create_table(self):
        """Method to create new sql-table
        """
        self.df.to_sql(self.table, self.conn)
        self.conn.commit()


    def split_tables(self, new_table, new_columns, select_columns, from_table):
        """Method to split table into a new table.
           First it creates the new table with specified columns, datatypes and Primary/Foreign keys.
           Second it inserts the selected data from old table and columns.

        Args:
            new_table (str): name of new table
            new_columns (str): names of new columns included Primary/Foreign keys
            select_columns (str): select which columns from old table to insert into new table.
            from_table (str): specify name of old table.
        """

        self.cur.execute(f"""CREATE TABLE IF NOT EXISTS {new_table} (
                            {new_columns}
                            )
                        """)
        self.cur.execute(f"""INSERT INTO {new_table} (
                                {select_columns})
                            SELECT DISTINCT {select_columns}
                            FROM {from_table}
                        """)
        self.conn.commit()


    def drop_columns(self, table, columns, new_columns):
        """Method to drop columns from old table that was splitted into new tables.
           self.temporary_table is a variable that just renames the old table to make a copy of it,
           to later be used to get the columns we want.
           In the end we delete the temporary_table and left is the original table with the columns we want.

        Args:
            table (str): name of table where columns will be dropped.
            columns (str): names of columns we want to include in the table
            new_columns (str): names of columns to be inserted in to the new table.
        """      
        self.temporary_table = table + '_old'

        self.cur.execute('PRAGMA foreign_keys=off;')
        self.cur.execute('BEGIN TRANSACTION;')
        self.cur.execute(f'ALTER TABLE {table} RENAME TO {self.temporary_table};')
        self.cur.execute(f"""CREATE TABLE {table}(
                             {columns}
                         );""")

        self.cur.execute(f"""INSERT INTO {table} (
                             {new_columns})
                        SELECT {new_columns}
                        FROM {self.temporary_table};""")

        self.cur.execute(f'DROP TABLE IF EXISTS {self.temporary_table}')
        self.conn.commit()
        self.cur.execute("PRAGMA foreign_keys=on;")

