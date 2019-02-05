#!/usr/bin/env python
# -*- coding:  utf-8 -*-


import os
import sqlite3
from collections import OrderedDict
# Database information
DB_NAME = "data"
DB_EXT = ".db"



# Paths
SOURCE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))  # \wwww\.
DB_PATH = os.path.join(SOURCE_PATH, "db", DB_NAME+DB_EXT)

#print(SOURCE_PATH)


class Database(object):
    def __init__(self, db_path=DB_PATH):

        self.__connection = type(sqlite3.connect)
        self.__selected_connection = None
        self.errors = None
        self.connect(db_path)


    def connect(self, db_path=DB_PATH):
        try:
            self.__connection = sqlite3.connect(db_path)
            self.__connection.row_factory = sqlite3.Row  # to call a row['table_name'] always before cursor()
            self.__selected_connection = self.__connection.cursor()


        except Exception as e:
            print (e.message)
            return None

    def is_connect(self):
        if self.__connection:
            return True
        else:
            return False

    def execute(self, sql):
        return self.__selected_connection.execute(sql)

    @staticmethod
    def __get_to_create_sql_data(table_name="objects", **kwargs):
        sql = range(5)
        sql[0] = "CREATE TABLE '{table}'".format(table=table_name)
        sql[1] = "("
        sql[2] = "id INTEGER PRIMARY KEY,"
        sql[3] = " ".join(str(_key) + " " + str(_type) + "," for _key, _type in kwargs.items())
        sql[3] = str(sql[3]).rstrip(",")
        sql[4] = ")"
        return " ".join(list(sql))

    @staticmethod
    def __get_to_write_sql_data(table_name, **kwargs):
        # write("table_name", ab=1, ff=11)
        sql = range(6)
        sql[0] = "INSERT INTO '{0}'".format(table_name)
        sql[1] = "("
        sql[2] = " ".join(_key + "," for _key, _value in kwargs.items())
        sql[2] = str(sql[2]).rstrip(",")
        sql[3] = ")" + " VALUES" + "("
        sql[4] = " ".join("'"+str(_value)+"'" + "," for _key, _value in kwargs.items())
        sql[4] = str(sql[4]).rstrip(",")  # deleted ,
        sql[5] = ")"
        sql = " ".join(list(sql))
        return sql

    @staticmethod
    def __get_to_read_sql_data(table_name):
        sql = range(2)
        sql[0] = "SELECT * FROM "
        sql[1] = "'"+table_name+"'"
        print " ".join(list(sql))
        return " ".join(list(sql))

    @staticmethod
    def __get_to_delete_sql_data(table_name,**kwargs):
        sql = range(3)
        sql[0] = "DELETE from '{table}'".format(table=table_name)
        sql[1] = "WHERE "
        sql[2] = " ".join(str(_key)+"="+ "'"+str(_value)+"'" for _key,_value in kwargs.items())
        return " ".join(list(sql))

    @staticmethod
    def __get_to_update_sql_data(table_name, field_name, old, new):
        sql = range(3)
        sql[0] = "UPDATE {table}".format(table=table_name)
        sql[1] = "SET {field}='{_new}'".format(field=field_name, _new=new)
        sql[2] = "WHERE {field}='{_old}'".format(field=field_name, _old=old)

        return " ".join(list(sql))

    # ## Executes ##

    # Create
    def create(self, table="objects", **kwargs):
        sql_data = self.__get_to_create_sql_data(table, **kwargs)
        self.execute(sql_data)

    # Write to table
    def write(self, table_name, **kwargs):
        sql_data = self.__get_to_write_sql_data(table_name, **kwargs)
        return self.execute(sql_data), sql_data

    # Delete
    def delete(self, table_name, **kwargs):
        sql_data = self.__get_to_delete_sql_data(table_name, **kwargs)
        return self.execute(sql_data),sql_data

    # Create Meta / Last sent data
    def create_meta(self):
        self.create("meta", modifiedDate="VARCHAR(50)", version="VARCHAR(50)")

    def read(self):
        if self.is_connect():
            pass
        else:
            pass

    def change(self,table_name,field,old,new):
        sql_data = self.__get_to_update_sql_data(table_name,field,old,new)
        print sql_data
        return self.execute(sql_data)


    def read_all_table(self, table_name):

        if self.is_connect():
            sql_data = self.__get_to_read_sql_data(table_name)
            fields = self.execute(sql_data).fetchall()
            print fields
            for verileri_cek in fields:
                print verileri_cek['name']
            return fields
        else:
            return None

    def save(self):
        self.__connection.commit()

    def close(self):
        self.__connection.close()



db1 = Database("db.db")
#db1.create("tableVarchar",name="VARCHAR(50)",soyad="VARCHAR(50)")
#db1.write("tableVarchar",name="merhaba",soyad="2322")


#db1.delete("tableVarchar",name="merhaba")
db1.read_all_table("tableVarchar")
db1.change("tableVarchar","soyad","@","MerhabaBurasi")

db1.save()
db1.close()



"""
def setup_db():
    db1.open()
    db1.create_table()
    db1.close()


#db1.open()
#db1.read_all_table("objects")
#db1.save()
#db1.close()

"""

