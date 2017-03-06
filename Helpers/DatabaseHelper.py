import mysql.connector
from Configs import dbconfig
class DatabaseHelper(object):
    def __init__(self):
        self.db = dbconfig.DATABASE['db'];
        self.host = dbconfig.DATABASE['host'];
        self.port = dbconfig.DATABASE['port'];
        self.user = dbconfig.DATABASE['user'];
        self.password = dbconfig.DATABASE['password'];
        self.type = dbconfig.DATABASE['type'];
        self.cnx = mysql.connector.connect(user=self.user, password=self.password, host=self.host,database=self.db)
        self.cursor = self.cnx.cursor()
    def query(self,queryString, queryParams=()):
        self.cursor.execute(queryString, queryParams)
        return self.cursor
    def __del__(self):
        self.cnx.close()
