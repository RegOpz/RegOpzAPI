from Helpers.DatabaseHelper import DatabaseHelper
from flask import url_for
from Models.Token import Token
# import bcrypt
from Constants.Status import *

class RegOpzUser(object):
    def __init__(self, user=None):
        if user:
            self.id = None
            self.name = user['name']
            self.password = user['password']
            self.role = user['role']
            self.first_name = user['first_name']
            self.last_name = user['last_name']
            self.contact_number = user['contact_number']
            self.email = user['email']
            self.ip = user['ip']
            self.image = None
            #5201json = {"name":"admin","first_name":"admin","last_name":"admin","password":"admin","contact_number":"8420403988","email":"admin@admin.com","ip":"1.1.1.1"}

    def save(self):
        queryString = \
            "INSERT INTO regopzuser (name,password,role_id,first_name,last_name,contact_number,email,ip,image) VALUES (%s,%s,(SELECT id from roles where role=%s),%s,%s,%s,%s,%s)"
        values = (
            self.name,
            self.password,
            self.role,
            self.first_name,
            self.last_name,
            self.contact_number,
            self.email,
            self.ip,
            self.image,
            )
        dbhelper = DatabaseHelper()
        try:
            rowid = dbhelper.transact(queryString, values)
            return self.get(rowid)
        except Exception:
            return { "msg": "Cannot add this user, please review the details" },400

    def get(self,userId=None):
        queryString = 'SELECT * FROM vuserdetails'
        dbhelper = DatabaseHelper()
        cur = dbhelper.query(queryString)
        data = cur.fetchall()
        if data:
            return data
        return NO_USER_FOUND

    def login(self, username, password):
        # This process cannot distinguish between Invalid password and Invalid username
        # hashpass = bcrypt.hashpw(base64.b64encode(hashlib.sha256(password).digest()), username)
        queryString = 'SELECT * FROM regopzuser WHERE name=%s AND password=%s'
        dbhelper = DatabaseHelper()
        cur = dbhelper.query(queryString, (username, password, ))
        data = cur.fetchone()
        if data:
            return Token().create(data)
        return {"msg": "Login failed"},403
