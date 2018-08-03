import numpy as np
FUNCTION_MAP = {
    "if":"xif",
    "sum":"xsum"
}

def xif(condition,if_clause,else_clause):
    return if_clause if condition else else_clause

def xsum(*args):
    pass