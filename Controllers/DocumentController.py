from flask_restful import Resource,abort
import os
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename
import uuid
from Constants.Status import *
from Helpers.DatabaseHelper import DatabaseHelper
UPLOAD_FOLDER = './uploads/templates'
ALLOWED_EXTENSIONS = set(['txt', 'xls', 'xlsx'])
class DocumentController(Resource):
    def allowed_file(self,filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    def get(self):
        return GET_IS_NOT_UPPORTED
    def post(self):
        if 'file' not in request.files:
            return NO_FILE_SELECTED
        file = request.files['file']
        if file and not self.allowed_file(file.filename):
            return FILE_TYPE_IS_NOT_ALLOWED
        filename = str(uuid.uuid4()) + "_" + secure_filename(file.filename)
        file.save(os.path.join(UPLOAD_FOLDER, filename))
        dbhelper = DatabaseHelper()
        addDocument = "INSERT INTO Document (id, file,uploaded_by,time_stamp,ip,comment) VALUES (NULL, %s, %s, NULL, %s, %s)"
        documentValues = (filename,1,'1.1.1.1','sample comment')
        lastRowId = dbhelper.transact(addDocument,documentValues)
        if lastRowId != 0:
            return {
                "id": lastRowId,
                "file": filename
            }
        return DATABASE_ERROR
