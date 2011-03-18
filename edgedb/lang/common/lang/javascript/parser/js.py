##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import sys

from semantix.utils import parsing

#import ast as jsast
from .. import ast as jsast

from . import keywords
#import keywords

# wrapping the Parsing classes?

class TokenMeta(parsing.TokenMeta):
    pass

class Token(parsing.Token, metaclass=TokenMeta):
    pass

class Nonterm(parsing.Nonterm):
    pass

class PrecedenceMeta(parsing.PrecedenceMeta):
    pass

class Precedence(parsing.Precedence, assoc='fail', metaclass=PrecedenceMeta):
    pass

# Precedence
class P_ADD_OP(Precedence, assoc='left', tokens=('PLUS', 'MINUS')):
    pass

class P_MUL_OP(Precedence, assoc='left', tokens=('STAR', 'SLASH', 'PERCENT')):
    pass

# Terminals

class T_ASSIGN(Token, lextoken='='):
    pass

class T_SEMICOLON(Token):
    pass

class T_COMMA(Token):
    pass

class T_LPAREN(Token):
    pass

class T_RPAREN(Token):
    pass

class T_LSBRACKET(Token):
    pass

class T_RSBRACKET(Token):
    pass

class T_LCBRACKET(Token):
    pass

class T_RCBRACKET(Token):
    pass

class T_ID(Token):
    pass

class T_STRING(Token):
    pass

class T_NUMBER(Token):
    pass

class T_PLUS(Token, lextoken='+'):
    pass

class T_MINUS(Token, lextoken='-'):
    pass

class T_STAR(Token, lextoken='*'):
    pass

class T_SLASH(Token, lextoken='/'):
    pass

class T_PERCENT(Token, lextoken='%'):
    pass

def _gen_keyword_tokens():
    # Define keyword tokens

    for val, (token, typ) in keywords.js_keywords.items():
        clsname = 'T_%s' % token
        cls = TokenMeta(clsname, (Token,), {'__module__': __name__}, token=token)
        setattr(sys.modules[__name__], clsname, cls)
_gen_keyword_tokens()

# Productions

class Program(Nonterm):
    "%start"

    def reduce_SourceElements(self, source):
        "%reduce SourceElements"
        self.val = jsast.ProgramNode(code = source.val)

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = jsast.ProgramNode(code = None)

class SourceElements(Nonterm):
    # SourceElement
    # | SourceElements SourceElement

    def reduce_SourceElement(self, *kids):
        "%reduce SourceElement"
        self.val = [kids[0].val]

    def reduce_SourceElements_SourceElement(self, *kids):
        "%reduce SourceElements SourceElement"
        self.val = kids[0].val + [kids[1].val]

class SourceElement(Nonterm):
    # Statement | FunctionDeclaration

    def reduce_Statement(self, *kids):
        "%reduce Statement"
        self.val = kids[0].val

#    def reduce_FunctionDeclaration(self, *kids):
#        "%reduce FunctionDeclaration"
#        self.val = kids[0].val

class Statement(Nonterm):
    # VariableStatement
    # | SEMICOLON
    # | BaseExpression SEMICOLON

    def reduce_VariableStatement(self, *kids):
        "%reduce VariableStatement"
        self.val = jsast.StatementNode(statement = kids[0].val)

    def reduce_SEMICOLON(self, *kids):
        "%reduce SEMICOLON"
        self.val = jsast.StatementNode(statement = None)

    def reduce_BaseExpression_SEMICOLON(self, *kids):
        "%reduce BaseExpression SEMICOLON"
        self.val = jsast.StatementNode(statement = kids[0].val)

class VariableStatement(Nonterm):
    # VAR VariableDeclarationList SEMICOLON

    def reduce_VAR_VariableDeclarationList_SEMICOLON(self, *kids):
        "%reduce VAR VariableDeclarationList SEMICOLON"
        self.val = jsast.VarDeclarationNode(vars = kids[1].val)

class VariableDeclarationList(Nonterm):
    # VariableDeclaration | VariableDeclarationList COMMA VariableDeclaration

    def reduce_VariableDeclaration(self, *kids):
        "%reduce VariableDeclaration"
        self.val = [kids[0].val]

    def reduce_VariableDeclarationList_COMMA_VariableDeclaration(self, *kids):
        "%reduce VariableDeclarationList COMMA VariableDeclaration"
        self.val = kids[0].val + [kids[2].val]

class VariableDeclaration(Nonterm):
    # ID | ID ASSIGN AssignmentExpression

    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = jsast.VarInitNode(name = kids[0].val, value = None)

    def reduce_ID_ASSIGN_AssignmentExpression(self, *kids):
        "%reduce ID ASSIGN AssignmentExpression"
        self.val = jsast.VarInitNode(name = kids[0].val, value = kids[2].val)

class PrimaryExpression(Nonterm):
    # this | ID | Literal
    # | ArrayLiteral | ObjectLiteral | LPAREN Expression RPAREN

    # identifiers, literals and 'this'
    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = jsast.IDNode(name = kids[0].val)

    def reduce_Literal(self, *kids):
        "%reduce Literal"
        self.val = kids[0].val

    def reduce_THIS(self, *kids):
        "%reduce THIS"
        self.val = jsast.ThisNode()

    # array and object literals
    def reduce_ArrayLiteral(self, *kids):
        "%reduce ArrayLiteral"
        self.val = kids[0].val

    def reduce_ObjectLiteral(self, *kids):
        "%reduce ObjectLiteral"
        self.val = kids[0].val

    # LPAREN ExpressionList RPAREN
    def reduce_LPAREN_ExpressionList_RPAREN(self, *kids):
        "%reduce LPAREN ExpressionList RPAREN"
        self.val = jsast.ExpressionListNode(expressions = kids[1].val)

class Literal(Nonterm):
    # NUMBER | STRING | NULL | TRUE | FALSE

    def reduce_NUMBER(self, *kids):
        "%reduce NUMBER"
        self.val = jsast.LiteralNode(value = kids[0].val)

    def reduce_STRING(self, *kids):
        "%reduce STRING"
        self.val = jsast.LiteralNode(value = kids[0].val)

    def reduce_TRUE(self, *kids):
        "%reduce TRUE"
        self.val = jsast.LiteralNode(value = True)

    def reduce_FALSE(self, *kids):
        "%reduce FALSE"
        self.val = jsast.LiteralNode(value = False)

    def reduce_NULL(self, *kids):
        "%reduce NULL"
        self.val = jsast.NullNode()

class ExpressionList(Nonterm):
    # AssignmentExpression | ExpressionList COMMA AssignemntExpression

    def reduce_AssignmentExpression(self, *kids):
        "%reduce AssignmentExpression"
        self.val = [kids[0].val]

    def reduce_ExpressionList_COMMA_AssignmentExpression(self, *kids):
        "%reduce ExpressionList COMMA AssignmentExpression"
        self.val = kids[0].val + [kids[2].val]

class NewExpression(Nonterm):
    # !!! incomplete
    #
    # PrimaryExpression

    def reduce_PrimaryExpression(self, *kids):
        "%reduce PrimaryExpression"
        self.val = kids[0].val

class CallExpression(Nonterm):
    # !!! incomplete
    #
    # PrimaryExpression Arguments
    # | CallExpression Arguments
    def reduce_PrimaryExpression_Arguments(self, *kids):
        "%reduce PrimaryExpression Arguments"
        self.val = jsast.CallNode(call = kids[0].val, arguments = kids[1].val)

    def reduce_CallExpression_Arguments(self, *kids):
        "%reduce CallExpression Arguments"
        self.val = jsast.CallNode(call = kids[0].val, arguments = kids[1].val)

class Arguments(Nonterm):
    # LPAREN RPAREN
    # | LPAREN ArgumentList RPAREN

    def reduce_LPAREN_RPAREN(self, *kids):
        "%reduce LPAREN RPAREN"
        self.val = []

    def reduce_LPAREN_ArgumentList_RPAREN(self, *kids):
        "%reduce LPAREN ArgumentList RPAREN"
        self.val = kids[1].val

class ArgumentList(Nonterm):
    # AssignmentExpression
    # | ArgumentList COMMA AssignmentExpression

    def reduce_AssignmentExpression(self, *kids):
        "%reduce AssignmentExpression"
        self.val = [kids[0].val]

    def reduce_ElementList_COMMA_ElisionOPT_AssignmentExpression(self, *kids):
        "%reduce ArgumentList COMMA AssignmentExpression"
        self.val = kids[0].val + [kids[2].val]


class LHSExpression(Nonterm):
    # NewExpression | CallExpression

    def reduce_NewExpression(self, *kids):
        "%reduce NewExpression"
        self.val = kids[0].val

    def reduce_CallExpression(self, *kids):
        "%reduce CallExpression"
        self.val = kids[0].val

class AssignmentExpression(Nonterm):
    # !!! just primary for now

    def reduce_BaseExpression(self, *kids):
        "%reduce BaseExpression"
        self.val = kids[0].val

class BaseExpression(Nonterm):
    # !!! incomplete

    def reduce_LHSExpression(self, *kids):
        "%reduce LHSExpression"
        self.val = kids[0].val

    def reduce_BaseExpression_PLUS_BaseExpression(self, *kids):
        "%reduce BaseExpression PLUS BaseExpression"
        self.val = jsast.BinExpressionNode(left = kids[0].val, op = '+', right = kids[2].val)

    def reduce_BaseExpression_MINUS_BaseExpression(self, *kids):
        "%reduce BaseExpression MINUS BaseExpression"
        self.val = jsast.BinExpressionNode(left = kids[0].val, op = '-', right = kids[2].val)

    def reduce_BaseExpression_STAR_BaseExpression(self, *kids):
        "%reduce BaseExpression STAR BaseExpression"
        self.val = jsast.BinExpressionNode(left = kids[0].val, op = '*', right = kids[2].val)

    def reduce_BaseExpression_SLASH_BaseExpression(self, *kids):
        "%reduce BaseExpression SLASH BaseExpression"
        self.val = jsast.BinExpressionNode(left = kids[0].val, op = '/', right = kids[2].val)

class ArrayLiteral(Nonterm):
    # LSBRACKET ElisionOPT RSBRACKET
    # | LSBRACKET ElementList RSBRACKET
    # | LSBRACKET ElementList COMMA Elision RSBRACKET

    def reduce_LSBRACKET_ElisionOPT_RSBRACKET(self, *kids):
        "%reduce LSBRACKET ElisionOPT RSBRACKET"
        self.val = jsast.ArrayLiteralNode(array = kids[1].val)

    def reduce_LSBRACKET_ElementList_RSBRACKET(self, *kids):
        "%reduce LSBRACKET ElementList RSBRACKET"
        self.val = jsast.ArrayLiteralNode(array = kids[1].val)

    def reduce_LSBRACKET_ElementList_COMMA_Elision_RSBRACKET(self, *kids):
        "%reduce LSBRACKET ElementList COMMA Elision RSBRACKET"
        self.val = jsast.ArrayLiteralNode(array = kids[1].val + kids[3].val)

class ElisionOPT(Nonterm):
    # <e> | Elision

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = []

    def reduce_Elision(self, *kids):
        "%reduce Elision"
        self.val = kids[0].val

class Elision(Nonterm):
    # COMMA | Elision COMMA

    def reduce_COMMA(self, *kids):
        "%reduce COMMA"
        self.val = [None]

    def reduce_Elision_COMMA(self, *kids):
        "%reduce Elision COMMA"
        self.val = kids[0].val + [None]

class ElementList(Nonterm):
    # ElisionOPT AssignmentExpression
    # | ElementList COMMA ElisionOPT AssignmentExpression

    def reduce_ElisionOPT_AssignmentExpression(self, *kids):
        "%reduce ElisionOPT AssignmentExpression"
        self.val = kids[0].val + [kids[1].val]

    def reduce_ElementList_COMMA_ElisionOPT_AssignmentExpression(self, *kids):
        "%reduce ElementList COMMA ElisionOPT AssignmentExpression"
        self.val = kids[0].val + kids[2].val + [kids[3].val]

class ObjectLiteral(Nonterm):
    # LCBRACKET RCBRACKET
    # | LCBRACKET PropertyNameAndValueList RCBRACKET
    # | LCBRACKET PropertyNameAndValueList COMMA RCBRACKET

    def reduce_LCBRACKET_RCBRACKET(self, *kids):
        "%reduce LCBRACKET RCBRACKET"
        self.val = jsast.ObjectLiteralNode(properties = [])

    def reduce_LCBRACKET_PropertyNameAndValueList_RCBRACKET(self, *kids):
        "%reduce LCBRACKET PropertyNameAndValueList RCBRACKET"
        self.val = jsast.ObjectLiteralNode(properties = kids[1].val)

    def reduce_LCBRACKET_PropertyNameAndValueList_COMMA_RCBRACKET(self, *kids):
        "%reduce LCBRACKET PropertyNameAndValueList COMMA RCBRACKET"
        self.val = jsast.ObjectLiteralNode(properties = kids[1].val)

class PropertyNameAndValueList(Nonterm):
    # PropertyAssignment | PropertyNameAndValueList COMMA PropertyAssignment

    def reduce_PropertyAssignemnt(self, *kids):
        "%reduce PropertyAssignemnt"
        self.val = [kids[0].val]

    def reduce_PropertyNameAndValueList_COMMA_PropertyAssignemnt(self, *kids):
        "%reduce PropertyNameAndValueList COMMA PropertyAssignemnt"
        self.val = kids[0].val + [kids[2].val]

class PropertyAssignemnt(Nonterm):
    # PropertyName SEMICOLON AssignmentExpression
    # | 'get' PropertyName LPAREN RPAREN LCBRACKET FunctionBody RCBRACKET
    # | 'set' PropertyName LPAREN ID RPAREN LCBRACKET FunctionBody RCBRACKET

    def reduce_SimpleProperty(self, *kids):
        "%reduce PropertyName SEMICOLON AssignmentExpression"
        self.val = jsast.SimplePropertyNode(name = kids[0].val, value = kids[2].val)

    def reduce_GetProperty(self, *kids):
        "%reduce ID PropertyName LPAREN RPAREN LCBRACKET FunctionBody RCBRACKET"
        if kids[0].val == 'get':
            self.val = jsast.GetPropertyNode(name = kids[1].val, function = kids[5].val)
        else:
            raise SyntaxError("'get' expected")

    def reduce_SetProperty(self, *kids):
        "%reduce ID PropertyName LPAREN ID RPAREN LCBRACKET FunctionBody RCBRACKET"
        if kids[0].val == 'set':
            self.val = jsast.SetPropertyNode(name = kids[1].val, param = kids[3].val, function = kids[6].val)
        else:
            raise SyntaxError("'set' expected")

class PropertyName(Nonterm):
    # IdentifierName | STRING | NUMBER

    def reduce_IdentifierName(self, *kids):
        "%reduce IdentifierName"
        self.val = kids[0].val

    def reduce_STRING(self, *kids):
        "%reduce STRING"
        self.val = kids[0].val

    def reduce_NUMBER(self, *kids):
        "%reduce NUMBER"
        self.val = kids[0].val

class IdentifierName(Nonterm):
    # ID | {keywords}

    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = kids[0].val

    # !!! need a hack to grab all the keywords

class FunctionBody(Nonterm):
    # !!! just empty now

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None

