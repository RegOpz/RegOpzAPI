from datetime import datetime
FUNCTION_MAP = {
    "if":"xif",
    "sum":"xsum",
    "str":"xstr",
    "date":"xdate"
}

def xif(condition,if_clause,else_clause):
    return if_clause if condition else else_clause

def xstr(val):
    if not val:val=""
    return '"'+str(val)+'"'

def xdate(col,input_format,output_format):
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
    return (datetime.strptime(str(col),input_format) if not col and str(col)!='' else datetime.strptime('19000101','%Y%d%m')).strftime(output_format)

def xsum(*args):
    pass
