from flask_restful import Resource,abort
import os
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename
import uuid
from Constants.Status import *
from Helpers.DatabaseHelper import DatabaseHelper
from Models.Document import Document
import datetime
UPLOAD_FOLDER = './uploads/templates'
ALLOWED_EXTENSIONS = set(['txt', 'xls', 'xlsx'])
class DocumentController(Resource):
    def allowed_file(self,filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    def get(self, doc_id=None):
        document = Document()
        if doc_id != None:
            return (document.get(doc_id))
        return (document.get())
    def post(self):
        if 'file' not in request.files:
            return NO_FILE_SELECTED
        file = request.files['file']
        if file and not self.allowed_file(file.filename):
            return FILE_TYPE_IS_NOT_ALLOWED
        filename = str(uuid.uuid4()) + "_" + secure_filename(file.filename)
        file.save(os.path.join(UPLOAD_FOLDER, filename))
        document = Document({
            'id': None,
            'file': filename,
            'uploaded_by': 1,
            'time_stamp': str (datetime.datetime.utcnow()),
            'ip': '1.1.1.1',
            'comment': "Sample comment by model"
        })
        return (document.add())
