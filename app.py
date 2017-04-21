from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS, cross_origin
from Configs import APIConfig
app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://dev:harry123@ec2-52-77-112-190.ap-southeast-1.compute.amazonaws.com/RegOpz'
CORS(app)
api = Api(app)
apiPath = APIConfig.APIPATH