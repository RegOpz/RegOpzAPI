import os
import uuid
from datetime import datetime
from flask import Flask, request
from flask_restful import Resource, abort
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = './uploads/source-files'


class LoadDataFileController(Resource):
    def post(self):
        file = request.files['data_file']
        if file:
            filename = str(uuid.uuid4()) + '_' +\
                str(datetime.utcnow().isoformat()).replace(
                    ':', '_') + '_' + secure_filename(file.filename)

            file.save(os.path.join(UPLOAD_FOLDER, filename).replace('\\', '/'))
            return {'msg': 'File Transferred Successfully', 'filename': filename}, 200
