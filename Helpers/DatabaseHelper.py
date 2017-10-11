import mysql.connector
from Configs import dbconfig
from Helpers.CustomMySQLConverter import CustomMySQLConverterClass
from mysql.connector.errors import Error

class DatabaseHelper(object):
    def __init__(self):
        self.db = dbconfig.DATABASE['db'];
        self.host = dbconfig.DATABASE['host'];
        self.port = dbconfig.DATABASE['port'];
        self.user = dbconfig.DATABASE['user'];
        self.password = dbconfig.DATABASE['password'];
        self.type = dbconfig.DATABASE['type'];
        self.cnx = mysql.connector.connect(user=self.user, password=self.password, host=self.host,database=self.db)
        self.cursor = self.cnx.cursor(dictionary=True)
        self.cursor.execute('set global max_allowed_packet=524288000')
        self.cnx.set_converter_class(CustomMySQLConverterClass)

    def query(self,queryString, queryParams=None):
        try:
            if queryParams != None:
                self.cursor.execute(queryString,queryParams)
            else:
                self.cursor.execute(queryString)
            #print(self.cursor.statement)
            return self.cursor
        except Error as e:
            print(self.cursor.statement)
            print(e)
            raise(e)

    def connection(self):
        return self.cnx

    def _cursor(self):
        return self.cursor

    def transact(self,queryString, queryParams=()):
        try:
            self.cursor.execute(queryString, queryParams)
            #print(self.cursor.statement)
            return self.cursor.lastrowid
        except Error as e:
            print(self.cursor.statement)
            print(e)
            raise(e)


    def commit(self):
        try:
            self.cnx.commit()
        except Error as e:
            print(e)
            raise(e)

    def rollback(self):
        try:
            self.cnx.rollback()
        except Error as e:
            print(e)
            raise(e)

    def transactmany(self,queryString,queryParams):
        try:
            self.cursor.executemany(queryString,queryParams)
            #print(self.cursor.statement)
            return self.cursor.lastrowid
        except Error as e:
            print(self.cursor.statement)
            print(e)
            raise(e)


    def __del__(self):
        self.commit()
        self.cursor.close()
        self.cnx.close()
