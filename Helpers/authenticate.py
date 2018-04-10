from flask import Flask, request, Response
from Models.Token import Token
def authenticate(func):
    def wrapper(*args, **kwargs):
        auth = request.headers.get('Authorization')
        if not auth:
            return {"msg":"Forbidden! No valid token/domain found!"},403
        else:
            try:
                num = Token().authenticate()
                if not num:
                	return {"msg":"Forbidden! Try generating new token"},403
            except Exception as e:
                return {"msg": str(e)}, 400
        ret = func(*args, **kwargs)
        return ret
    return wrapper
