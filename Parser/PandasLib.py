import numpy as np
FUNCTION_MAP = {
    "if":"pif",
    "sum":"psum",
    "str":"pstr"
}

def pif(condition,if_clause,else_clause):
    return np.where(condition,if_clause,else_clause)

def psum(*args):
    pass

def pstr(col):
    col.fillna("",inplace=True)
    return col.apply(lambda x:'"'+str(x)+'"')