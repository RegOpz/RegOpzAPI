from networkx.classes.digraph import DiGraph
from Parser.Tokenizer import ExcelParser, f_token,ExcelParserTokens as ept
import Parser.ExcelLib as excellib
from Parser.ExcelLib import *
import collections
import math
from Helpers.utils import if_null_zero,if_null_blank,round_value

# def if_null_zero(value):
#     return 0 if value is None else value
#
# def if_null_blank(value):
#     return '""' if value is None else value
#
# def round_value(number_to_round,option):
#     if option=='' or option == None:
#         option='NONE'
#     if option=='CEIL':
#         rounded_number=math.ceil(number_to_round)
#     elif option=='FLOOR':
#         rounded_number = math.floor(number_to_round)
#     elif option=='TRUNC':
#         rounded_number=math.trunc(number_to_round)
#     elif 'DECIMAL' in option:
#         decimal_point=int(option.replace('DECIMAL',''))
#         rounded_number=round(number_to_round,decimal_point)
#     else:
#         rounded_number=number_to_round
#
#     return rounded_number

class Context(object):
    """A small context object that nodes in the AST can use to emit code"""
    def __init__(self,df):
        self.df=df

class ASTNode(object):
    """A generic node in the AST"""

    def __init__(self, token):
        super(ASTNode, self).__init__()
        self.token = token

    def __str__(self):
        return self.token.tvalue

    def __getattr__(self, name):
        return getattr(self.token, name)

    def children(self, ast):
        args = list(ast.predecessors(self))
        args = sorted(args, key=lambda x: ast.node[x]['pos'])
        # args.reverse()
        return args

    def parent(self, ast):
        args = list(ast.successors(self))
        return args[0] if args else None

    def emit(self, ast, context=None):
        """Emit code"""
        self.token.tvalue


class OperatorNode(ASTNode):
    def __init__(self, *args):
        super(OperatorNode, self).__init__(*args)

        # convert the operator to python equivalents
        self.opmap = {
            "^": "**",
            "=": "==",
            "&": "+",
            "": "+"  # union
        }

    def emit(self, ast, context=None):
        xop = self.tvalue

        # Get the arguments
        args = self.children(ast)

        op = self.opmap.get(xop, xop)

        if self.ttype == ept.TOK_TYPE_OP_PRE:
            return "-" + args[0].emit(ast, context=context)

        parent = self.parent(ast)
        ss = args[0].emit(ast, context=context) + op + args[1].emit(ast, context=context)
        # avoid needless parentheses
        if parent and not isinstance(parent, FunctionNode):
            ss = "(" + ss + ")"

        return ss


class OperandNode(ASTNode):
    def __init__(self, *args):
        super(OperandNode, self).__init__(*args)

    def emit(self, ast, context=None):
        t = self.tsubtype

        if t == ept.TOK_SUBTYPE_LOGICAL:
            return str(self.tvalue.lower() == "true")
        elif t == ept.TOK_SUBTYPE_TEXT or t == ept.TOK_SUBTYPE_ERROR:
            # if the string contains quotes, escape them
            val = self.tvalue.replace('"', '\\"')
            return '"' + val + '"'
        else:
            return str(self.tvalue)


class RangeNode(OperandNode):
    """Represents a spreadsheet cell or range, e.g., A5 or B3:C20"""

    def __init__(self, *args):
        super(RangeNode, self).__init__(*args)

    def emit(self, ast, context=None):
        # resolve the range into cells
        strn=self.tvalue
        return strn


class FunctionNode(ASTNode):
    """AST node representing a function call"""

    def __init__(self, *args):
        super(FunctionNode, self).__init__(*args)
        self.numargs = 0

        # map  excel functions onto their python equivalents
        self.funmap = excellib.FUNCTION_MAP

    def emit(self, ast, context):
        fun = self.tvalue.lower()
        strn = ''
        # Get the arguments
        args = self.children(ast)
        # map to the correct name
        f = self.funmap.get(fun, fun)
        strn = f + "(" + ",".join([n.emit(ast, context=context) for n in args]) + ")"

        return strn


def create_node(t):
    """Simple factory function"""
    if t.ttype == ept.TOK_TYPE_OPERAND:
        if t.tsubtype == ept.TOK_SUBTYPE_RANGE:
            return RangeNode(t)
        else:
            return OperandNode(t)
    elif t.ttype == ept.TOK_TYPE_FUNCTION:
        return FunctionNode(t)
    elif t.ttype in [ept.TOK_TYPE_OP_IN,ept.TOK_TYPE_OP_PRE,ept.TOK_TYPE_OP_POST]:
        return OperatorNode(t)
    else:
        return ASTNode(t)


class Operator:
    """Small wrapper class to manage operators during shunting yard"""

    def __init__(self, value, precedence, associativity):
        self.value = value
        self.precedence = precedence
        self.associativity = associativity


def shunting_yard(expression):
    """
    Tokenize an excel formula expression into reverse polish notation

    Core algorithm taken from wikipedia with varargs extensions from
    http://www.kallisti.net.nz/blog/2008/02/extension-to-the-shunting-yard-algorithm-to-allow-variable-numbers-of-arguments-to-functions/
    """
    # remove leading =
    if expression.startswith('='):
        expression = expression[1:]

    p = ExcelParser();
    p.parse(expression)
    #print(p.prettyprint())

    # insert tokens for '(' and ')', to make things clearer below
    tokens = []
    for t in p.tokens.items:
        if t.ttype == ept.TOK_TYPE_FUNCTION and t.tsubtype ==ept.TOK_SUBTYPE_START:
            t.tsubtype = ""
            tokens.append(t)
            tokens.append(f_token('(', 'arglist', 'start'))
        elif t.ttype == ept.TOK_TYPE_FUNCTION and t.tsubtype ==ept.TOK_SUBTYPE_STOP:
            tokens.append(f_token(')', 'arglist', 'stop'))
        elif t.ttype == ept.TOK_TYPE_SUBEXPR and t.tsubtype == ept.TOK_SUBTYPE_START:
            t.tvalue = '('
            tokens.append(t)
        elif t.ttype == ept.TOK_TYPE_SUBEXPR and t.tsubtype == ept.TOK_SUBTYPE_STOP:
            t.tvalue = ')'
            tokens.append(t)
        else:
            tokens.append(t)

    # print "tokens: ", "|".join([x.tvalue for x in tokens])

    # http://office.microsoft.com/en-us/excel-help/calculation-operators-and-precedence-HP010078886.aspx
    operators = {}
    operators[':'] = Operator(':', 8, 'left')
    operators[''] = Operator(' ', 8, 'left')
    operators[','] = Operator(',', 8, 'left')
    operators['u-'] = Operator('u-', 7, 'left')  # unary negation
    operators['%'] = Operator('%', 6, 'left')
    operators['^'] = Operator('^', 5, 'left')
    operators['*'] = Operator('*', 4, 'left')
    operators['/'] = Operator('/', 4, 'left')
    operators['+'] = Operator('+', 3, 'left')
    operators['-'] = Operator('-', 3, 'left')
    operators['&'] = Operator('&', 2, 'left')
    operators['='] = Operator('=', 1, 'left')
    operators['<'] = Operator('<', 1, 'left')
    operators['>'] = Operator('>', 1, 'left')
    operators['<='] = Operator('<=', 1, 'left')
    operators['>='] = Operator('>=', 1, 'left')
    operators['<>'] = Operator('<>', 1, 'left')

    output = collections.deque()
    stack = []
    were_values = []
    arg_count = []

    for t in tokens:
        if t.ttype == ept.TOK_TYPE_OPERAND:

            output.append(create_node(t))
            if were_values:
                were_values.pop()
                were_values.append(True)

        elif t.ttype == ept.TOK_TYPE_FUNCTION:

            stack.append(t)
            arg_count.append(0)
            if were_values:
                were_values.pop()
                were_values.append(True)
            were_values.append(False)

        elif t.ttype == ept.TOK_TYPE_ARGUMENT:

            while stack and (stack[-1].tsubtype != ept.TOK_SUBTYPE_START):
                output.append(create_node(stack.pop()))

            if were_values.pop(): arg_count[-1] += 1
            were_values.append(False)

            if not len(stack):
                raise Exception("Mismatched or misplaced parentheses")

        elif t.ttype in [ept.TOK_TYPE_OP_IN,ept.TOK_TYPE_OP_PRE,ept.TOK_TYPE_OP_POST]:

            if t.ttype == ept.TOK_TYPE_OP_PRE and t.tvalue == "-":
                o1 = operators['u-']
            else:
                o1 = operators[t.tvalue]

            while stack and stack[-1].ttype in [ept.TOK_TYPE_OP_IN,ept.TOK_TYPE_OP_PRE,ept.TOK_TYPE_OP_POST]:

                if stack[-1].ttype == ept.TOK_TYPE_OP_PRE and stack[-1].tvalue == "-":
                    o2 = operators['u-']
                else:
                    o2 = operators[stack[-1].tvalue]

                if ((o1.associativity == "left" and o1.precedence <= o2.precedence)
                    or
                        (o1.associativity == "right" and o1.precedence < o2.precedence)):

                    output.append(create_node(stack.pop()))
                else:
                    break

            stack.append(t)

        elif t.tsubtype == ept.TOK_SUBTYPE_START:
            stack.append(t)

        elif t.tsubtype == ept.TOK_SUBTYPE_STOP:

            while stack and stack[-1].tsubtype != ept.TOK_SUBTYPE_START:
                output.append(create_node(stack.pop()))

            if not stack:
                raise Exception("Mismatched or misplaced parentheses")

            stack.pop()

            if stack and stack[-1].ttype == ept.TOK_TYPE_FUNCTION:
                f = create_node(stack.pop())
                a = arg_count.pop()
                w = were_values.pop()
                if w: a += 1
                f.num_args = a
                # print f, "has ",a," args"
                output.append(f)

    while stack:
        if stack[-1].tsubtype == ept.TOK_SUBTYPE_START or stack[-1].tsubtype == ept.TOK_SUBTYPE_STOP:
            raise Exception("Mismatched or misplaced parentheses")

        output.append(create_node(stack.pop()))

    # print "Stack is: ", "|".join(stack)
    # print "Ouput is: ", "|".join([x.tvalue for x in output])

    # convert to list
    result = [x for x in output]
    return result


def build_ast(expression):
    """build an AST from an Excel formula expression in reverse polish notation"""

    # use a directed graph to store the tree
    G = DiGraph()

    stack = []

    for n in expression:
        # Since the graph does not maintain the order of adding nodes/edges
        # add an extra attribute 'pos' so we can always sort to the correct order
        if isinstance(n, OperatorNode):
            if n.ttype == ept.TOK_TYPE_OP_IN:
                arg2 = stack.pop()
                arg1 = stack.pop()
                G.add_node(arg1, pos=1)
                G.add_node(arg2, pos=2)
                G.add_edge(arg1, n)
                G.add_edge(arg2, n)
            else:
                arg1 = stack.pop()
                G.add_node(arg1, pos=1)
                G.add_edge(arg1, n)

        elif isinstance(n, FunctionNode):
            args = [stack.pop() for _ in range(n.num_args)]
            args.reverse()
            for i, a in enumerate(args):
                G.add_node(a, pos=i)
                G.add_edge(a, n)
                # for i in range(n.num_args):
                #    G.add_edge(stack.pop(),n)
        else:
            G.add_node(n, pos=0)

        stack.append(n)

    return G, stack.pop()

def tree(table = {}, **kwargs):
    debug = False
    format_flag = False

    kwlist = kwargs.keys()

    if "debug" in kwlist:
        debug = kwargs["debug"]
    if "format_flag" in kwlist:
        format_flag = kwargs["format_flag"] == "Y"

    def is_not_formula(formula):
        p=ExcelParser()
        p.parse(formula)
        tokens=p.ret_tokens()
        #print(len(tokens),tokens[0].ttype,tokens[0].tsubtype)

        if len(tokens)==1 and tokens[0].ttype ==ept.TOK_TYPE_OPERAND and \
           tokens[0].tsubtype in (ept.TOK_SUBTYPE_TEXT,ept.TOK_SUBTYPE_LOGICAL,ept.TOK_SUBTYPE_NUMBER):
           return tokens[0].tsubtype
        else:
            return False


    def dfs(root,G):
        if not isinstance(root,RangeNode) and isinstance(root,OperandNode):
            #print(root.tvalue)
            return root.emit(G)
        elif isinstance(root,(float,int)):
            return str(root)
        elif isinstance(root,str) and is_not_formula(root):
            if is_not_formula(root)==ept.TOK_SUBTYPE_NUMBER:
                return root
            root=eval(root)
            root = root.replace('"', '\\"')
            return '"'+root+'"'
        elif isinstance(root,RangeNode):
            ref=root.emit(G)
            #print(ref)
            if ref in eTree.keys():
                formula=eTree[ref]
                #print(formula,type(formula))
                if not isinstance(formula,RangeNode)and isinstance(formula,OperandNode):
                    return root.emit(G)
                elif isinstance(formula,(int,float)):
                    return str(formula)
                elif isinstance(formula,str) and is_not_formula(formula):
                    #print(is_not_formula(formula))
                    if is_not_formula(formula) == ept.TOK_SUBTYPE_NUMBER:
                        return formula
                    formula=eval(formula)
                    formula = formula.replace('"', '\\"')
                    return '"'+formula+'"'
                #print(formula)
                #dfs_formula=dfs(*formula["tree"])
                #print("DFS formula:",dfs_formula)
                round_val = roundOff(eval(str(if_null_zero(dfs(*formula["tree"])))),formula["rounding"],formula["scale"])
                #round_val=eval(dfs(*formula["tree"]))
                #return round_val
                if isinstance(round_val,(int,float)):
                    return str(round_val)
                elif isinstance(round_val,str):
                    return '"' + round_val + '"'

            else:
                return None
        elif isinstance(root,OperatorNode):
            xop = root.tvalue

            # Get the arguments
            args = root.children(G)
            #print(args)

            op = root.opmap.get(xop, xop)

            if root.ttype == ept.TOK_TYPE_OP_PRE:
                return "-" + str(if_null_zero(dfs(args[0],G)))
            parent = root.parent(G)
            print(args[0].emit(G),args[1].emit(G))
            ss = str(if_null_zero(dfs(args[0],G))) + op + str(if_null_zero(dfs(args[1],G)))
            # avoid needless parentheses
            if parent and not isinstance(parent, FunctionNode):
                ss = "(" + ss + ")"
            #print(ss)
            return ss

        elif isinstance(root,FunctionNode):
            funmap=excellib.FUNCTION_MAP
            fun = root.tvalue.lower()
            strn = ''
            # Get the arguments
            args = root.children(G)
            # map to the correct name
            f = funmap.get(fun, fun)
            strn = f + "(" + ",".join([str(if_null_blank(dfs(n,G))) for n in args]) + ")"
            return strn

    def exprToTree(expression):
        pexpr=shunting_yard(expression)
        G,root=build_ast(pexpr)
        return root,G

    def roundOff(value, rounding, scale):
        try:
            if scale and rounding:
              return round_value(float(if_null_zero(value)) / float(scale), rounding)
            elif not scale and rounding:
                return round_value(float(if_null_zero(value)), rounding)
            elif scale and not rounding:
                return float(if_null_zero(value))/float(scale)
            else:
                return value
        except Exception as e:
            #raise e
            return None

    eTree = {}
    for key, value in table.items():
        rounding = value["rounding_option"] if format_flag and value["rounding_option"]!="NONE" else None
        scale = value["reporting_scale"] if format_flag and value["reporting_scale"]!="NONE"else None
        formula = str(value["formula"])
        eTree[key] = {
            "tree": exprToTree(formula),
            "rounding": rounding,
            "scale": scale
        }

    for key, value in eTree.items():
        #print(value["tree"])
        (root,G)=value["tree"]
        if not isinstance(root,OperandNode) or isinstance(root,RangeNode):
            #round_val=roundOff(eval(dfs(*value["tree"])),value["rounding"],value["scale"])
            round_val=roundOff(eval(str(if_null_zero(dfs(*value["tree"])))),value["rounding"],value["scale"])
            print(round_val,type(round_val))
            #round_val=roundOff(round_val,value["rounding"],value["scale"])
            #print(round_val,type(round_val))
            if isinstance(round_val, (int, float)):
                eTree[key]=str(round_val)
            elif isinstance(round_val, str):
                eTree[key]='"' + round_val + '"'



    if debug:
       print(eTree)

if __name__ == '__main__':
    table = {}
    table["C1"] = { "formula": "5*4+3", "rounding_option": "DECIMAL0", "reporting_scale": "1" }
    table["C2"] = { "formula": "20+C1", "rounding_option": "DECIMAL0", "reporting_scale": "10" }
    table["C3"] = { "formula": "C1*C2", "rounding_option": "DECIMAL0", "reporting_scale": "10" }
    table["C4"]={ "formula": "if(C1*C2>0,\"YES\",\"NO\")", "rounding_option": "NONE", "reporting_scale": "NONE" }
    table["C5"] = {"formula": "if(C4=\"YES\",if(C1+C3>10,\"Y\",\"N\"),\"N\")", "rounding_option": "NONE", "reporting_scale": "NONE"}
    table["C7"] = {"formula": "if(C1*C3+C9>0,if(C8=\"Y\",\"YES\",\"NO\"),\"NO\")", "rounding_option": "NONE",
                   "reporting_scale": "NONE"}
    table["C11"]={"formula": "C12+C14+C16", "rounding_option": "NONE","reporting_scale": "NONE"}
    table["C13"] = {"formula": "C11+C14+C15+C16+C19", "rounding_option": "NONE", "reporting_scale": "NONE"}
    tree(table, debug=True,format_flag='Y')