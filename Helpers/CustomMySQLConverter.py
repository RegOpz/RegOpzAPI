import mysql.connector

class CustomMySQLConverterClass(mysql.connector.conversion.MySQLConverter):
    def _int64_to_mysql(self,value):
        return int(value)
    def _ndarray_to_mysql(self,value):
        return str(value)
    def _float64_to_mysql(self,value):
        return float(value)
    def _int32_to_mysql(self,value):
        return int(value)