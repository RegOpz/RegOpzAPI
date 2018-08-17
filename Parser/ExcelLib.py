FUNCTION_MAP = {
    "if":"xif",
    "sum":"xsum",
    "str":"xstr",
}

def xif(condition,if_clause,else_clause):
    return if_clause if condition else else_clause

def xstr(val):
    if not val:val=""
    return '"'+str(val)+'"'

def xsum(*args):
    pass