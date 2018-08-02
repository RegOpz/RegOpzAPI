from Helpers.DatabaseHelper import DatabaseHelper
from flask import url_for, request
from Models.Token import Token
from bcrypt import hashpw, gensalt
from Constants.Status import *
import json
from Helpers.authenticate import *
from Helpers.utils import autheticateTenant
from datetime import datetime

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
        self.domain_info=autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.dbhelper = DatabaseHelper(self.tenant_info)
        if user and user['password'] == user['passwordConfirm']:
            self.id = True
            self.name = user['name']
            self.password = user['password']
            self.hashedpassword = hashpw(self.password.encode('utf-8'), gensalt())
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
        queryParams = (self.name, self.hashedpassword, self.role, self.status, self.first_name,
            self.last_name, self.contact_number, self.email, self.ip, self.image,)
        try:
            rowid = self.dbhelper.transact(queryString, queryParams)
            self.dbhelper.commit()
            return { "msg": "Added user successfully, please contact Admin to activate",
                    "donotUseMiddleWare": True },200
        except Exception as e:
            print(e)
            return { "msg": "Cannot add this user, please review the details" },400

    def get(self, userId = None, labellist = False):
        queryString = "SELECT name, role, first_name, last_name, email, contact_number, image, status FROM regopzuser\
            JOIN (roles) ON (regopzuser.role_id = roles.id) WHERE status != 'Deleted'"
        queryParams = ()
        if userId:
            queryString += " AND regopzuser.name = %s"
            queryParams = (userId, )
        cur = self.dbhelper.query(queryString, queryParams)
        data = cur.fetchall()
        if data:
            if labellist:
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
        else:
            data = form

        if data:
            queryString = "UPDATE regopzuser SET role_id=(SELECT id from roles WHERE role=%s), first_name=%s, last_name=%s, email=%s,\
                contact_number=%s, status=%s"
            queryParams = (data['role'], data['first_name'], data['last_name'], data['email'], \
                data['contact_number'], data['status'])
            if 'password' in data.keys() and data['password'] and data['password']==data['passwordConfirm']:
                queryString += ",password=%s"
                queryParams = queryParams + (hashpw(data['password'].encode('utf-8'), gensalt()),)

            queryString += " WHERE name=%s"
            queryParams = queryParams + (data['name'],)

            try:
                rowId = self.dbhelper.transact(queryString, queryParams)
                self.dbhelper.commit()
                return { "msg": "Successfully updated user details." },200
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
                return { "msg": "Username already exists.", "donotUseMiddleWare": True },200
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
        try:
            # This process cannot distinguish between Invalid password and Invalid username
            # hashpass = bcrypt.hashpw(base64.b64encode(hashlib.sha256(password).digest()), username)
            queryString = "SELECT r.role, u.* FROM regopzuser u JOIN (roles r) ON (u.role_id = r.id)\
                WHERE name=%s AND status='Active'"
            cur = self.dbhelper.query(queryString, (username, ))
            data = cur.fetchone()
            if data:
                # If user data exist then check the hashed password value
                password_entered = password.encode('utf-8')
                # print("hashpw {}".format(hashpw(password.encode('utf-8'), gensalt())))
                hashedpassword = data['password'].encode('utf-8')
                if hashpw(password_entered,hashedpassword)==hashedpassword:
                    tym = data['pwd_change_tym']
                    tym_now = datetime.now()
                    diff = ((tym_now - tym).total_seconds()) / (60 * 60 * 24)
                    #id = self.domain_info['tenant_id']
                    id = 'admin'
                    sql = "select expiry_period from pwd_policy where tenant_id = '{}'".format(id)
                    db = DatabaseHelper()
                    limit = db.query(sql).fetchone()
                    limit = limit['expiry_period']
                    #print(limit)
                    if diff > limit:
                        return{'msg': 'Password expired! , Please change password to continue'}, 403
                    elif (diff < limit) and (diff > (limit - 10)):
                         return Token().create(data)
                    else:
                        return Token().create(data)

            return {"msg": "Login failed", "donotUseMiddleWare": True },403
        except Exception as e:
            print(str(e))
            return {"msg": "Login failed", "donotUseMiddleWare": True },403
