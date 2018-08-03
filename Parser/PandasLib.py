import numpy as np
FUNCTION_MAP = {
    "if":"pif",
    "sum":"psum"
}

def pif(condition,if_clause,else_clause):
    return np.where(condition,if_clause,else_clause)

def psum(*args):
    pass