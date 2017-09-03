# Module: Expression Tree for Spreadsheet
# @function: tree
# @param table: A set of reference and their formula/rules
# @param @optional debug: Prints table (Given) and values (Evalutated) iff True
# @returns Evalutated table

def tree(table = {}, debug = False):

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
                return float(self.node.value)
            elif self.node.key == "ref":
                ref = self.node.value
                if ref and ref in eTree:
                    eq = eTree[ref]
                    if type(eq) == float:
                        return eq
                    if eq.node.key == "val":
                        return eq.node.value
                    val = eq.dfs()
                    eTree[ref] = Vertex(Node("val", val))
                    return val
                else:
                    raise ValueError("Invalid Operation defined: Found Reference Error", ref)
            else:
                if self.left:
                    left = self.left.dfs()
                if self.right:
                    right = self.right.dfs()
                if self.node.key == "op":
                    if self.node.value == "+":
                        return (left + right)
                    elif self.node.value == "-":
                        return (left - right)
                    elif self.node.value == "*":
                        return (left * right)
                    elif self.node.value == "/":
                        try:
                            return (left / right)
                        except Exception:
                            raise ValueError("Invalid Operation defined: Found Arithmetic Error")
                    else:
                        raise ValueError("Invalid Operation defined: Found Invalid Error")

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

    if debug:
        print("Table:", table)

    eTree = {}
    for key, value in table.items():
        eTree[key] = exprToTree(value)

    for key, value in eTree.items():
        eTree[key] = value if type(value) == float else value.dfs()

    if debug:
        print("Evalutated:", eTree)

    return eTree

# Testing Script
if __name__ == '__main__':
    table = {}
    table["C1"] = "5 * 4 + 3"
    table["C2"] = "20 + C1"
    table["C3"] = "C1 * C2"
    tree(table, True)
