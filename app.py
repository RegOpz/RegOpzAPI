from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS, cross_origin
from Configs import APIConfig
import logging
from logging.handlers import RotatingFileHandler

app = Flask(__name__)
CORS(app)
api = Api(app)
apiPath = APIConfig.APIPATH

formatter = logging.Formatter("[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
handler = RotatingFileHandler('./logs/foo.log', maxBytes=10000, backupCount=100)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
app.logger.addHandler(handler)
