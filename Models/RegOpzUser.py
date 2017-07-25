from Helpers.DatabaseHelper import DatabaseHelper
from flask import url_for, request
from Models.Token import Token
# import bcrypt
from Constants.Status import *

labelList = {
    'role': "Role",
    'first_name': "First Name",
    'last_name': "Last Name",
    'email': "Email",
    'contact_number': "Contact Number",
    'status': "Status"
}

class RegOpzUser(object):
    def __init__(self, user = None):
        self.dbhelper = DatabaseHelper()
        if user and user['password'] == user['passwordConfirm']:
            self.id = True
            self.name = user['name']
            self.password = user['password']
            self.role = user['role'] if 'role' in user else "Default"
            self.status = user['status'] if 'status' in user else "Unspecified"
            self.first_name = user['first_name']
            self.last_name = user['last_name']
            self.contact_number = user['contact_number'] if 'contact_number' in user else None
            self.email = user['email']
            self.ip = request.remote_addr
            self.image = None
        else:
            self.id = False

    def save(self):
        queryString = "INSERT INTO regopzuser (name,password,role_id,status_id,first_name,last_name,\
            contact_number,email,ip,image) VALUES (%s,%s,(SELECT id from roles where role=%s),\
            (SELECT id from status_def where status=%s),%s,%s,%s,%s,%s,%s)"
        queryParams = (self.name, self.password, self.role, self.status, self.first_name,
            self.last_name, self.contact_number, self.email, self.ip, self.image,)
        try:
            rowid = self.dbhelper.transact(queryString, queryParams)
            self.dbhelper.commit()
            return { "msg": "Added user successfully, please contact Admin to activate" },200
        except Exception as e:
            print(e)
            return { "msg": "Cannot add this user, please review the details" },400

    def get(self, userId = None):
        queryString = "SELECT name, role, first_name, last_name, email, contact_number, image, status FROM regopzuser\
            JOIN (roles, status_def) ON (regopzuser.role_id = roles.id AND regopzuser.status_id = status_def.id)\
            WHERE status_def.status != 'Deleted'"
        if userId:
            queryString += " AND regopzuser.name = %s"
            queryParams = (userId, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
        else:
            cur = self.dbhelper.query(queryString)
            data = cur.fetchall()
        if data:
            userList = []
            for user in data:
                infoList = []
                for key in user:
                    if key in labelList:
                        infoObj = {
                            'title': labelList[key],
                            'value': user[key]
                        }
                        infoList.append(infoObj)
                userObj = {
                    'username': user['name'],
                    'name': user['first_name'],
                    'info': infoList
                }
                userList.append(userObj)
            return userList
        return NO_USER_FOUND

    def getUserList(self, userId = None):
        if userId:
            print(userId)
            queryString = "SELECT name FROM regopzuser WHERE name=%s"
            queryParams = (userId, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return { "msg": "Username already exists." },200
            return {},200

    def login(self, username, password):
        # This process cannot distinguish between Invalid password and Invalid username
        # hashpass = bcrypt.hashpw(base64.b64encode(hashlib.sha256(password).digest()), username)
        queryString = "SELECT * FROM regopzuser JOIN status_def ON (regopzuser.status_id = status_def.id) \
            WHERE name=%s AND password=%s AND status='Active'"
        dbhelper = DatabaseHelper()
        cur = dbhelper.query(queryString, (username, password, ))
        data = cur.fetchone()
        if data:
            return Token().create(data)
        return {"msg": "Login failed"},403
