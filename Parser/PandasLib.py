import numpy as np
FUNCTION_MAP = {
    "rate":"rate",
     "if":"xif",
    "sum":"xsum"
}


def rate(currency):
    pass

def xif(condition,if_clause,else_clause):
    return np.where(condition,if_clause,else_clause)

def xsum(*args):
    pass