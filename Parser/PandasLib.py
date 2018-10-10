import numpy as np
from datetime import datetime
FUNCTION_MAP = {
    "if":"pif",
    "sum":"psum",
    "str":"pstr",
    "date":"pdate"
}

def pif(condition,if_clause,else_clause):
    return np.where(condition,if_clause,else_clause)

def psum(*args):
    pass

def pstr(col):
    col.fillna("",inplace=True)
    return col.apply(lambda x:'"'+str(x)+'"')

def pdate(col,input_format,output_format):
    col.fillna("",inplace=True)
    # python Date formats:
    # %d - date e.g. 01, 12
    # %m - Month e.g. 01, 11
    # %Y - Year as YYYY; %y - year as YY
    # %a - Day e.g. Mon, Tue; %A - Day in full e.g. Monday, Friday
    # %b - Mon e.g Jan, Feb; %B - Month e.g. January, March
    # %H - Hour in 24 hr format; %I - hour as 12 hr format
    # %M - Minutes
    # %S - Second
    # %p - am or pm in 12 hr format

    return col.apply(lambda x: (datetime.strptime(str(x),input_format) if str(x)!='' else datetime.strptime('19000101','%Y%d%m')).strftime(output_format))
