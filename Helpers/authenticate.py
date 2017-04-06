from flask import Flask, request, Response
from Models.Token import Token
def authenticate(func):
    def wrapper(*args, **kwargs):
        auth = request.headers.get('Authorization')     
        if not auth:
            #return Response('Could not verify your access level for that URL.\nYou have to login with proper credentials', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})       
            return {"msg":"Forbidden"},403
        else:                    	                      
            num = Token().authenticate(auth)	       
            if not num:
            	return {"msg":"Forbidden"},403
        ret = func(*args, **kwargs)        
        return ret
    return wrapper