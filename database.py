#!/usr/bin/env python
# -*- coding:  utf-8 -*-


import os
import sqlite3




# Path
DB_PATH = os.path.abspath(os.path.join('.','database.db'))

#print(DB_PATH)


class Database(object):
    def __init__(self, db_path=DB_PATH):
        self.__connection = None
        self.__selected_connection = None
        self.__table_name = ""
        self.__field_name = ""
        self.errors = None
        self.__connect(db_path)

    def __connect(self, db_path=DB_PATH):
        try:
            self.__connection = sqlite3.connect(db_path)
            self.__connection.row_factory = sqlite3.Row  # to call a row['__table_name'] always before cursor()
            self.__selected_connection = self.__connection.cursor()

        except Exception as e:
            print (e.message)
            self.errors = e.message
            return None

    def is_connect(self):
        if self.__connection:
            return True
        else:
            return False

    def __execute(self, sql):
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
        # write("__table_name", ab=1, ff=11)
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
    def create(self, **kwargs):
        sql_data = self.__get_to_create_sql_data(self.__table_name, **kwargs)
        self.__execute(sql_data)

    # Write to table
    def write(self, **kwargs):
        sql_data = self.__get_to_write_sql_data(self.__table_name, **kwargs)
        return self.__execute(sql_data), sql_data

    # Delete
    def delete(self, **kwargs):
        sql_data = self.__get_to_delete_sql_data(self.__table_name, **kwargs)
        return self.__execute(sql_data), sql_data

    def change(self, old, new):
        sql_data = self.__get_to_update_sql_data(self.__table_name, self.__field_name, old, new)
        return self.__execute(sql_data)

    def read(self):
        sql_data = self.__get_to_read_sql_data(self.__table_name)
        fields = self.__execute(sql_data).fetchall()
        return fields

    def last_row_id(self):
        return self.__selected_connection.lastrowid

    # Set Functions
    def set_table_name(self, table_name):
        self.__table_name = table_name

    def set_field_name(self, field_name):
        self.__field_name = field_name

    def save(self):
        self.__connection.commit()

    def close(self):
        self.__connection.close()

if __name__ == '__main__':
    db1 = Database()
    db1.set_table_name("table_table")
    db1.create(name="VARCHAR(50)", lastname="VARCHAR(50)")
    db1.write(name="Hi",lastname="World")

    db1.set_field_name("name")
    db1.change("Hi","Hello")

    #db1.delete(name="Hi")


    db1.save()
    DB = db1.read()
    print DB[0]["name"] + " " + DB[0]["lastname"]
    db1.close()
