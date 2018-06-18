from networkx.classes.digraph import DiGraph
from Parser.Tokenizer import ExcelParser, f_token,ExcelParserTokens
import Parser.PandasLib as excellib
import collections

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

        if self.ttype == ExcelParserTokens.TOK_TYPE_OP_PRE:
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

        if t == ExcelParserTokens.TOK_SUBTYPE_LOGICAL:
            return str(self.tvalue.lower() == "true")
        elif t == ExcelParserTokens.TOK_SUBTYPE_TEXT or t == ExcelParserTokens.TOK_SUBTYPE_ERROR:
            # if the string contains quotes, escape them
            val = self.tvalue.replace('"', '\\"')
            return '"' + val + '"'
        else:
            return str(self.tvalue)


class RangeNode(OperandNode):
    """Represents a spreadsheet cell or range, e.g., A5 or B3:C20"""

    def __init__(self, *args):
        super(RangeNode, self).__init__(*args)

    def emit(self, ast, context):
        # resolve the range into cells
        str=context.df+"['{}']".format(self.tvalue)
        return str


class FunctionNode(ASTNode):
    """AST node representing a function call"""

    def __init__(self, *args):
        super(FunctionNode, self).__init__(*args)
        self.numargs = 0

        # map  excel functions onto their python equivalents
        self.funmap = excellib.FUNCTION_MAP

    def emit(self, ast, context):
        fun = self.tvalue.lower()
        str = ''
        # Get the arguments
        args = self.children(ast)
        # map to the correct name
        f = self.funmap.get(fun, fun)
        str = f + "(" + ",".join([n.emit(ast, context=context) for n in args]) + ")"

        return str


def create_node(t):
    """Simple factory function"""
    if t.ttype == ExcelParserTokens.TOK_TYPE_OPERAND:
        if t.tsubtype == ExcelParserTokens.TOK_SUBTYPE_RANGE:
            return RangeNode(t)
        else:
            return OperandNode(t)
    elif t.ttype == ExcelParserTokens.TOK_TYPE_FUNCTION:
        return FunctionNode(t)
    elif t.ttype in [ExcelParserTokens.TOK_TYPE_OP_IN,ExcelParserTokens.TOK_TYPE_OP_PRE,ExcelParserTokens.TOK_TYPE_OP_POST]:
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
        if t.ttype == ExcelParserTokens.TOK_TYPE_FUNCTION and t.tsubtype ==ExcelParserTokens.TOK_SUBTYPE_START:
            t.tsubtype = ""
            tokens.append(t)
            tokens.append(f_token('(', 'arglist', 'start'))
        elif t.ttype == ExcelParserTokens.TOK_TYPE_FUNCTION and t.tsubtype ==ExcelParserTokens.TOK_SUBTYPE_STOP:
            tokens.append(f_token(')', 'arglist', 'stop'))
        elif t.ttype == ExcelParserTokens.TOK_TYPE_SUBEXPR and t.tsubtype == ExcelParserTokens.TOK_SUBTYPE_START:
            t.tvalue = '('
            tokens.append(t)
        elif t.ttype == ExcelParserTokens.TOK_TYPE_SUBEXPR and t.tsubtype == ExcelParserTokens.TOK_SUBTYPE_STOP:
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
        if t.ttype == ExcelParserTokens.TOK_TYPE_OPERAND:

            output.append(create_node(t))
            if were_values:
                were_values.pop()
                were_values.append(True)

        elif t.ttype == ExcelParserTokens.TOK_TYPE_FUNCTION:

            stack.append(t)
            arg_count.append(0)
            if were_values:
                were_values.pop()
                were_values.append(True)
            were_values.append(False)

        elif t.ttype == ExcelParserTokens.TOK_TYPE_ARGUMENT:

            while stack and (stack[-1].tsubtype != ExcelParserTokens.TOK_SUBTYPE_START):
                output.append(create_node(stack.pop()))

            if were_values.pop(): arg_count[-1] += 1
            were_values.append(False)

            if not len(stack):
                raise Exception("Mismatched or misplaced parentheses")

        elif t.ttype in [ExcelParserTokens.TOK_TYPE_OP_IN,ExcelParserTokens.TOK_TYPE_OP_PRE,ExcelParserTokens.TOK_TYPE_OP_POST]:

            if t.ttype == ExcelParserTokens.TOK_TYPE_OP_PRE and t.tvalue == "-":
                o1 = operators['u-']
            else:
                o1 = operators[t.tvalue]

            while stack and stack[-1].ttype in [ExcelParserTokens.TOK_TYPE_OP_IN,ExcelParserTokens.TOK_TYPE_OP_PRE,ExcelParserTokens.TOK_TYPE_OP_POST]:

                if stack[-1].ttype == ExcelParserTokens.TOK_TYPE_OP_PRE and stack[-1].tvalue == "-":
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

        elif t.tsubtype == ExcelParserTokens.TOK_SUBTYPE_START:
            stack.append(t)

        elif t.tsubtype == ExcelParserTokens.TOK_SUBTYPE_STOP:

            while stack and stack[-1].tsubtype != ExcelParserTokens.TOK_SUBTYPE_START:
                output.append(create_node(stack.pop()))

            if not stack:
                raise Exception("Mismatched or misplaced parentheses")

            stack.pop()

            if stack and stack[-1].ttype == ExcelParserTokens.TOK_TYPE_FUNCTION:
                f = create_node(stack.pop())
                a = arg_count.pop()
                w = were_values.pop()
                if w: a += 1
                f.num_args = a
                # print f, "has ",a," args"
                output.append(f)

    while stack:
        if stack[-1].tsubtype == ExcelParserTokens.TOK_SUBTYPE_START or stack[-1].tsubtype == ExcelParserTokens.TOK_SUBTYPE_STOP:
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
            if n.ttype == ExcelParserTokens.TOK_TYPE_OP_IN:
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


if __name__ == '__main__':
    inputs2 = ['1',
    #            '-2.3',
    #            '"Suchibrata"',
    #            "1+2",
    #            "true",
    #            '"28-NOV-2018"'
               'S1C1+S1C2+S1C3',
               'SUM(S1C1:S1C15)',
               'SUM(S1C1,S1C2,S1C3)',
               'SUM(IF(A*B>=0,A),IF(B>=0,B,0))',
               'IF(S1C1*S2C1 >0,"Yes","No")',
               'if(business_date ="20180615",buy_amount*rate(buy_currency),sell_amount*rate(sell_currency))',
                ]
    for i in inputs2:
        e=shunting_yard(i)
        G,root=build_ast(e)
        context = Context('df')
        print("Python code:",root.emit(G,context=context))