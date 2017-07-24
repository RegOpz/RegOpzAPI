from Helpers.DatabaseHelper import DatabaseHelper
from flask import url_for, request
from Models.Token import Token
# import bcrypt
from Constants.Status import *

labelList = {
    'name': "User Name",
    'role': "Role",
    'first_name': "First Name",
    'last_name': "Last Name",
    'email': "Email",
    'contact_number': "Contact Number",
    'status': "Status",
    "User Name": 'name',
    "Role": 'role',
    "First Name": 'first_name',
    "Last Name": 'last_name',
    "Email": 'email',
    "Contact Number": 'contact_number',
    "Status": 'status'
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

    def save(self):
        queryString = "INSERT INTO regopzuser (name,password,role_id,status,first_name,last_name,\
            contact_number,email,ip,image) VALUES (%s,%s,(SELECT id from roles where role=%s),%s,%s,%s,%s,%s,%s,%s)"
        queryParams = (self.name, self.password, self.role, self.status, self.first_name,
            self.last_name, self.contact_number, self.email, self.ip, self.image,)
        try:
            rowid = self.dbhelper.transact(queryString, queryParams)
            self.dbhelper.commit()
            return { "msg": "Added user successfully, please contact Admin to activate" },200
        except Exception as e:
            print(e)
            return { "msg": "Cannot add this user, please review the details" },400

    def get(self, userId = None, update = False):
        queryString = "SELECT name, role, first_name, last_name, email, contact_number, image, status FROM regopzuser\
            JOIN (roles) ON (regopzuser.role_id = roles.id) WHERE status != 'Deleted'"
        queryParams = ()
        if userId:
            queryString += " AND regopzuser.name = %s"
            queryParams = (userId, )
        cur = self.dbhelper.query(queryString, queryParams)
        data = cur.fetchall()
        if data:
            if update:
                return data
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
                    'name': user['first_name'],
                    'username': user['name'],
                    'info': infoList
                }
                userList.append(userObj)
            return userList
        return NO_USER_FOUND

    def update(self, form = None):
        if form and labelList['name'] in form:
            data = {}
            for key in form:
                label = labelList[key]
                data[label] = form[key]
            queryString = "UPDATE regopzuser SET role_id=(SELECT id from roles WHERE role=%s), first_name=%s, last_name=%s, email=%s,\
                contact_number=%s, status=%s WHERE name=%s"
            queryParams = (data['role'], data['first_name'], data['last_name'], data['email'], \
                data['contact_number'], data['status'], data['name'])
            try:
                rowId = self.dbhelper.transact(queryString, queryParams)
                self.dbhelper.commit()
                return { "msg": "Successfully updated details." },200
            except Exception as e:
                print(e)
                return { "msg": "Cannot update this user, please review the details" },400
        return NO_USER_FOUND

    def getUserList(self, userId = None):
        if userId:
            queryString = "SELECT name FROM regopzuser WHERE name=%s"
            queryParams = (userId, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return { "msg": "Username already exists." },200
            return {},200

    def changeStatus(self, userId = None):
        if userId:
            queryString = "SELECT status FROM regopzuser WHERE name=%s"
            queryParams = (userId,)
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                prevStat = data['status']
                nextStat = "Deleted" if prevStat == "Active" else "Active"
                queryString = "UPDATE regopzuser SET status=%s WHERE name=%s"
                queryParams = (nextStat, userId)
                try:
                    rowId = self.dbhelper.transact(queryString, queryParams)
                    self.dbhelper.commit()
                    return { "msg": "Status updated successfully." },200
                except Exception:
                    return { "msg": "Failed to update status." },200
            return { "msg": "Invalid credentials recieved." },301

    def login(self, username, password):
        # This process cannot distinguish between Invalid password and Invalid username
        # hashpass = bcrypt.hashpw(base64.b64encode(hashlib.sha256(password).digest()), username)
        queryString = "SELECT r.role, u.* FROM regopzuser u JOIN (roles r) ON (u.role_id = r.id)\
            WHERE name=%s AND password=%s AND status='Active'"
        dbhelper = DatabaseHelper()
        cur = dbhelper.query(queryString, (username, password, ))
        data = cur.fetchone()
        if data:
            return Token().create(data)
        return {"msg": "Login failed"},403
