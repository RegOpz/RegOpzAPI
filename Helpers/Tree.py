# Module: Expression Tree for Spreadsheet
# @function: tree
# @param table: A set of reference and their formula/rules along with rounding option
# @kwarg @optional debug: Prints table (Given) and values (Evalutated) iff True
# @returns Evalutated table

import Helpers.utils as util

def tree(table = {}, **kwargs):
    debug = False
    format_flag = False

    kwlist = kwargs.keys()

    if "debug" in kwlist:
        debug = kwargs["debug"]
    if "format_flag" in kwlist:
        format_flag = kwargs["format_flag"] == "Y"

    # Object Definitions
    class Node(object):
        def __init__(self, key = None, value = None):
            self.key = key
            self.value = value

    class Vertex(object):
        def __init__(self, node):
            self.node = node
            self.left = None
            self.right = None

        def dfs(self):
            left = 0
            right = 0
            if self.node.key == "val":
                return self.node.value
            elif self.node.key == "ref":
                ref = self.node.value
                if ref in eTree.keys():
                    eq = eTree[ref]
                    if type(eq) == float:
                        return eq
                    expt, rounding, scale = eq["tree"], eq["rounding"], eq["scale"]
                    round_val = roundOff(expt.dfs(), rounding, scale)
                    eTree[ref] = round_val
                    return round_val
                else:
                    return 0.0 # Some reference error need to be fixed in database
                    # raise ValueError("Invalid Operation defined: Found Reference Error {0}".format(ref))
            else:
                if self.left:
                    left = self.left.dfs()
                if self.right:
                    right = self.right.dfs()
                if self.node.key == "op":
                    opcode = self.node.value
                    if opcode == "+":
                        return (left + right)
                    elif opcode == "-":
                        return (left - right)
                    elif opcode == "*":
                        return (left * right)
                    elif opcode == "/":
                        try:
                            return (left / right)
                        except Exception as e:
                            raise ValueError("Invalid Operand defined: Found \
Arithmetic Error {0}".format(e))
                    else:
                        raise ValueError("Invalid Operation defined: Found \
Invalid Operator {0}".format(opcode))

    # Method Definitions
    def roundOff(value: float, rounding: str, scale: float):
        try:
            return util.round_value(float(util.if_null_zero(value) / scale), rounding)
        except Exception:
            return 0.0

    def isOperator(token):
        return (token in ('+', '-', '*', '/'))

    def isValue(token):
        try:
            float(token)
            return True
        except ValueError:
            return False

    def infixToPostfix(expr: str):
        tokenList = expr.split()
        prec = {
            "*": 3,
            "/": 3,
            "+": 2,
            "-": 2,
            "(": 1
        }
        opStack = []
        postfix = []

        for token in tokenList:
            if token == '(':
                opStack.append(token)
            elif token == ')':
                top = opStack.pop()
                while top != '(':
                    postfix.append(top)
                    top = opStack.pop()
            elif not isOperator(token):
                postfix.append(token)
            else:
                length = len(opStack)
                while length != 0 and \
                prec[opStack[length - 1]] >= prec[token]:
                    postfix.append(opStack.pop())
                    length = len(opStack)
                opStack.append(token)
        while len(opStack) != 0:
            postfix.append(opStack.pop())
        return " ".join(postfix)

    def exprToTree(expr: str):
        pexpr = infixToPostfix(expr)
        explist = pexpr.split()
        pstack = []

        for token in explist:
            if isOperator(token):
                root = Vertex(Node("op", token))
                root.right = pstack.pop()
                root.left = pstack.pop()
                pstack.append(root)
            else:
                root = None
                if isValue(token):
                    root = Vertex(Node("val", float(token)))
                else:
                    root = Vertex(Node("ref", token))
                pstack.append(root)
        return pstack.pop()

    # Normal Execution
    if debug:
        print("Table:", table)

    eTree = {}
    for key, value in table.items():
        rounding = value["rounding_option"] if format_flag else "NONE"
        scale = float(value["reporting_scale"]) if format_flag else 1.0
        formula = str(value["formula"]).replace("+", " + ").replace("-", " - ") \
.replace("*", " * ").replace("/", " / ").replace("(", " ( ").replace(")", " ) ")
        eTree[key] = {
            "tree":     exprToTree(formula),
            "rounding": rounding,
            "scale":    scale
        }

    for key, value in eTree.items():
        if type(value) != float:
            expt, rounding, scale = value["tree"], value["rounding"], value["scale"]
            round_val = roundOff(expt.dfs(), rounding, scale)
            eTree[key] = round_val

    if debug:
        print("Evalutated:", eTree)

    return eTree

# Testing Script
if __name__ == '__main__':
    table = {}
    table["C1"] = { "formula": "5*4+3", "rounding_option": "DECIMAL0", "reporting_scale": "1" }
    table["C2"] = { "formula": "20+C1", "rounding_option": "DECIMAL0", "reporting_scale": "10" }
    table["C3"] = { "formula": "C1*C2", "rounding_option": "DECIMAL0", "reporting_scale": "10" }
    tree(table, debug=True)
