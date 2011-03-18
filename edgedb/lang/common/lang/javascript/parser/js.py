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
class P_SHIFT(Precedence, assoc='left', tokens=('LEFTSHIFT', 'SIGNRIGHTSHIFT', 'ZERORIGHTSHIFT')):
    pass

class P_ADD(Precedence, assoc='left', tokens=('PLUS', 'MINUS')):
    pass

class P_MULTIPLY(Precedence, assoc='left', tokens=('STAR', 'SLASH', 'PERCENT')):
    pass

# Terminals

class T_SEMICOLON(Token):
    pass

class T_COLON(Token):
    pass

class T_COMMA(Token):
    pass

class T_DOT(Token):
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

class T_CIRCUM(Token, lextoken='^'):
    pass

class T_LESS(Token, lextoken='<'):
    pass

class T_MORE(Token, lextoken='>'):
    pass

class T_ASSIGN(Token, lextoken='='):
    pass

class T_BANG(Token, lextoken='!'):
    pass

class T_TILDE(Token, lextoken='~'):
    pass

class T_AND(Token, lextoken='&'):
    pass

class T_OR(Token, lextoken='|'):
    pass

class T_QUESTION(Token, lextoken='?'):
    pass

class T_PLUSPLUS(Token, lextoken='++'):
    pass

class T_MINUSMINUS(Token, lextoken='--'):
    pass

class T_LEFTSHIFT(Token, lextoken='<<'):
    pass

class T_SIGNRIGHTSHIFT(Token, lextoken='>>'):
    pass

class T_ZERORIGHTSHIFT(Token, lextoken='>>>'):
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
        self.val = jsast.ProgramNode(code=source.val)

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = jsast.ProgramNode(code=None)

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
    # !!! incomplete
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
    # | ExpressionList SEMICOLON

    def reduce_VariableStatement(self, *kids):
        "%reduce VariableStatement"
        self.val = jsast.StatementNode(statement=kids[0].val)

    def reduce_SEMICOLON(self, *kids):
        "%reduce SEMICOLON"
        self.val = jsast.StatementNode(statement=None)

    def reduce_ExpressionList_SEMICOLON(self, *kids):
        "%reduce ExpressionList SEMICOLON"
        self.val = jsast.StatementNode(statement=kids[0].val)

class VariableStatement(Nonterm):
    # VAR VariableDeclarationList SEMICOLON

    def reduce_VAR_VariableDeclarationList_SEMICOLON(self, *kids):
        "%reduce VAR VariableDeclarationList SEMICOLON"
        self.val = jsast.VarDeclarationNode(vars=kids[1].val)

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
        self.val = jsast.VarInitNode(name=kids[0].val, value=None)

    def reduce_ID_ASSIGN_AssignmentExpression(self, *kids):
        "%reduce ID ASSIGN AssignmentExpression"
        self.val = jsast.VarInitNode(name=kids[0].val, value=kids[2].val)

class PrimaryExpression(Nonterm):
    # this | ID | Literal
    # | ArrayLiteral | ObjectLiteral | LPAREN Expression RPAREN

    # identifiers, literals and 'this'
    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = jsast.IDNode(name=kids[0].val)

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
        self.val = jsast.ParenthesisNode(expression=kids[1].val)

class Literal(Nonterm):
    # NUMBER | STRING | NULL | TRUE | FALSE

    def reduce_NUMBER(self, *kids):
        "%reduce NUMBER"
        self.val = jsast.NumericLiteralNode(value=kids[0].val)

    def reduce_STRING(self, *kids):
        "%reduce STRING"
        self.val = jsast.StringLiteralNode(value=kids[0].val)

    def reduce_TRUE(self, *kids):
        "%reduce TRUE"
        self.val = jsast.BooleanLiteralNode(value=True)

    def reduce_FALSE(self, *kids):
        "%reduce FALSE"
        self.val = jsast.BooleanLiteralNode(value=False)

    def reduce_NULL(self, *kids):
        "%reduce NULL"
        self.val = jsast.NullNode()

class ExpressionList(Nonterm):
    # AssignmentExpression | ExpressionList COMMA AssignemntExpression

    def reduce_AssignmentExpression(self, *kids):
        "%reduce AssignmentExpression"
        self.val = jsast.ExpressionListNode(expressions=[kids[0].val])

    def reduce_ExpressionList_COMMA_AssignmentExpression(self, *kids):
        "%reduce ExpressionList COMMA AssignmentExpression"
        kids[0].val.expressions += [kids[2].val]
        #self.val = kids[0].val + [kids[2].val]

class MemberExpression(Nonterm):
    #!!! incomplete
    #
    # PrimaryExpression
    # | MemberExpression LSBRACKET ExpressionList RSBRACKET
    # | MemberExpression DOT IdentifierName
    # | NEW MemberExpression Arguments

    def reduce_PrimaryExpression(self, *kids):
        "%reduce PrimaryExpression"
        self.val = kids[0].val

    def reduce_MemberExpression_LSBRACKET_ExpressionList_RSBRACKET(self, *kids):
        "%reduce MemberExpression LSBRACKET ExpressionList RSBRACKET"
        self.val = jsast.SBracketExpressionNode(list=kids[0].val, element=kids[2].val)

    def reduce_MemberExpression_DOT_IdentifierName(self, *kids):
        "%reduce MemberExpression DOT IdentifierName"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op=".", right=kids[2].val)

    def reduce_NEW_MemberExpression_Arguments(self, *kids):
        "%reduce NEW MemberExpression Arguments"
        self.val = jsast.NewNode(expression=kids[1].val, arguments=kids[2].val)


class NewExpression(Nonterm):
    # MemberExpression | NEW NewExpression

    def reduce_MemberExpression(self, *kids):
        "%reduce MemberExpression"
        self.val = kids[0].val

    def reduce_NEW_NewExpression(self, *kids):
        "%reduce NEW NewExpression"
        self.val = jsast.NewNode(expression=kids[1].val, arguments=None)

class CallExpression(Nonterm):
    # MemberExpression Arguments
    # | CallExpression Arguments
    # | CallExpression LSBRACKET ExpressionList RSBRACKET
    # | CallExpression DOT IdentifierName

    def reduce_MemberExpression_Arguments(self, *kids):
        "%reduce MemberExpression Arguments"
        self.val = jsast.CallNode(call=kids[0].val, arguments=kids[1].val)

    def reduce_CallExpression_Arguments(self, *kids):
        "%reduce CallExpression Arguments"
        self.val = jsast.CallNode(call=kids[0].val, arguments=kids[1].val)

    def reduce_CallExpression_LSBRACKET_ExpressionList_RSBRACKET(self, *kids):
        "%reduce CallExpression LSBRACKET ExpressionList RSBRACKET"
        self.val = jsast.SBracketExpressionNode(list=kids[0].val, element=kids[2].val)

    def reduce_CallExpression_DOT_IdentifierName(self, *kids):
        "%reduce CallExpression DOT IdentifierName"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op=".", right=kids[2].val)

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

class PostfixExpression(Nonterm):
    # !!! line terminator not checked
    #
    # LHSExpression
    # | LHSExpression [no LineTerminator here] PLUSPLUS
    # | LHSExpression [no LineTerminator here] MINUSMINUS

    def reduce_LHSExpression(self, *kids):
        "%reduce LHSExpression"
        self.val = kids[0].val

    def reduce_LHSExpression_PLUSPLUS(self, *kids):
        "%reduce LHSExpression PLUSPLUS"
        self.val = jsast.PostfixExpressionNode(expression=kids[0].val, op="++")

    def reduce_LHSExpression_MINUSMINUS(self, *kids):
        "%reduce LHSExpression MINUSMINUS"
        self.val = jsast.PostfixExpressionNode(expression=kids[0].val, op="--")

class UnaryExpression(Nonterm):
    # PostfixExpression
    # | DELETE UnaryExpression
    # | VOID UnaryExpression
    # | TYPEOF UnaryExpression
    # | ++ UnaryExpression
    # | -- UnaryExpression
    # | + UnaryExpression
    # | - UnaryExpression
    # | ~ UnaryExpression
    # | ! UnaryExpression

    def reduce_PostfixExpression(self, *kids):
        "%reduce PostfixExpression"
        self.val = kids[0].val

    def reduce_DELETE_UnaryExpression(self, *kids):
        "%reduce DELETE UnaryExpression"
        self.val = jsast.DeleteNode(expression=kids[1].val)

    def reduce_VOID_UnaryExpression(self, *kids):
        "%reduce VOID UnaryExpression"
        self.val = jsast.VoidNode(expression=kids[1].val)

    def reduce_TYPEOF_UnaryExpression(self, *kids):
        "%reduce TYPEOF UnaryExpression"
        self.val = jsast.TypeOfNode(expression=kids[1].val)

    def reduce_PLUSPLUS_UnaryExpression(self, *kids):
        "%reduce PLUSPLUS UnaryExpression"
        self.val = jsast.PrefixExpressionNode(op='++', expression=kids[1].val)

    def reduce_MINUSMINUS_UnaryExpression(self, *kids):
        "%reduce MINUSMINUS UnaryExpression"
        self.val = jsast.PrefixExpressionNode(op='--', expression=kids[1].val)

    def reduce_PLUS_UnaryExpression(self, *kids):
        "%reduce PLUS UnaryExpression"
        self.val = jsast.PrefixExpressionNode(op='+', expression=kids[1].val)

    def reduce_MINUS_UnaryExpression(self, *kids):
        "%reduce MINUS UnaryExpression"
        self.val = jsast.PrefixExpressionNode(op='-', expression=kids[1].val)

    def reduce_TILDE_UnaryExpression(self, *kids):
        "%reduce TILDE UnaryExpression"
        self.val = jsast.PrefixExpressionNode(op='~', expression=kids[1].val)

    def reduce_BANG_UnaryExpression(self, *kids):
        "%reduce BANG UnaryExpression"
        self.val = jsast.PrefixExpressionNode(op='!', expression=kids[1].val)

class ArithmeticAnsShiftExpression(Nonterm):
    # ArithmeticAnsShiftExpression + ArithmeticAnsShiftExpression
    # | ArithmeticAnsShiftExpression - ArithmeticAnsShiftExpression
    # | ArithmeticAnsShiftExpression * ArithmeticAnsShiftExpression
    # | ArithmeticAnsShiftExpression / ArithmeticAnsShiftExpression
    # | ArithmeticAnsShiftExpression % ArithmeticAnsShiftExpression
    # | ArithmeticAnsShiftExpression << ArithmeticAnsShiftExpression
    # | ArithmeticAnsShiftExpression >> ArithmeticAnsShiftExpression
    # | ArithmeticAnsShiftExpression >>> ArithmeticAnsShiftExpression

    def reduce_UnaryExpression(self, *kids):
        "%reduce UnaryExpression"
        self.val = kids[0].val

    def reduce_ArithmeticAnsShiftExpression_PLUS_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression PLUS ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='+', right=kids[2].val)

    def reduce_ArithmeticAnsShiftExpression_MINUS_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression MINUS ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='-', right=kids[2].val)

    def reduce_ArithmeticAnsShiftExpression_STAR_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression STAR ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='*', right=kids[2].val)

    def reduce_ArithmeticAnsShiftExpression_SLASH_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression SLASH ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='/', right=kids[2].val)

    def reduce_ArithmeticAnsShiftExpression_PERCENT_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression PERCENT ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='%', right=kids[2].val)

    def reduce_ArithmeticAnsShiftExpression_LEFTSHIFT_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression LEFTSHIFT ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='<<', right=kids[2].val)

    def reduce_ArithmeticAnsShiftExpression_SIGNRIGHTSHIFT_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression SIGNRIGHTSHIFT ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='>>', right=kids[2].val)

    def reduce_ArithmeticAnsShiftExpression_ZERORIGHTSHIFT_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression ZERORIGHTSHIFT ArithmeticAnsShiftExpression"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op='>>>', right=kids[2].val)

class AssignmentExpression(Nonterm):
    # !!! just primary for now

    def reduce_ArithmeticAnsShiftExpression(self, *kids):
        "%reduce ArithmeticAnsShiftExpression"
        self.val = kids[0].val

class ArrayLiteral(Nonterm):
    # LSBRACKET ElisionOPT RSBRACKET
    # | LSBRACKET ElementList RSBRACKET
    # | LSBRACKET ElementList COMMA Elision RSBRACKET

    def reduce_LSBRACKET_ElisionOPT_RSBRACKET(self, *kids):
        "%reduce LSBRACKET ElisionOPT RSBRACKET"
        self.val = jsast.ArrayLiteralNode(array=kids[1].val)

    def reduce_LSBRACKET_ElementList_RSBRACKET(self, *kids):
        "%reduce LSBRACKET ElementList RSBRACKET"
        self.val = jsast.ArrayLiteralNode(array=kids[1].val)

    def reduce_LSBRACKET_ElementList_COMMA_Elision_RSBRACKET(self, *kids):
        "%reduce LSBRACKET ElementList COMMA Elision RSBRACKET"
        self.val = jsast.ArrayLiteralNode(array=kids[1].val + kids[3].val)

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
        self.val = jsast.ObjectLiteralNode(properties=[])

    def reduce_LCBRACKET_PropertyNameAndValueList_RCBRACKET(self, *kids):
        "%reduce LCBRACKET PropertyNameAndValueList RCBRACKET"
        self.val = jsast.ObjectLiteralNode(properties=kids[1].val)

    def reduce_LCBRACKET_PropertyNameAndValueList_COMMA_RCBRACKET(self, *kids):
        "%reduce LCBRACKET PropertyNameAndValueList COMMA RCBRACKET"
        self.val = jsast.ObjectLiteralNode(properties=kids[1].val)

class PropertyNameAndValueList(Nonterm):
    # PropertyAssignment | PropertyNameAndValueList COMMA PropertyAssignment

    def reduce_PropertyAssignemnt(self, *kids):
        "%reduce PropertyAssignemnt"
        self.val = [kids[0].val]

    def reduce_PropertyNameAndValueList_COMMA_PropertyAssignemnt(self, *kids):
        "%reduce PropertyNameAndValueList COMMA PropertyAssignemnt"
        self.val = kids[0].val + [kids[2].val]

class PropertyAssignemnt(Nonterm):
    # PropertyName COLON AssignmentExpression
    # | 'get' PropertyName LPAREN RPAREN LCBRACKET FunctionBody RCBRACKET
    # | 'set' PropertyName LPAREN ID RPAREN LCBRACKET FunctionBody RCBRACKET

    def reduce_SimpleProperty(self, *kids):
        "%reduce PropertyName COLON AssignmentExpression"
        self.val = jsast.SimplePropertyNode(name=kids[0].val, value=kids[2].val)

    def reduce_GetProperty(self, *kids):
        "%reduce ID PropertyName LPAREN RPAREN LCBRACKET FunctionBody RCBRACKET"
        if kids[0].val == 'get':
            self.val = jsast.GetPropertyNode(name=kids[1].val, function=kids[5].val)
        else:
            raise SyntaxError("'get' expected")

    def reduce_SetProperty(self, *kids):
        "%reduce ID PropertyName LPAREN ID RPAREN LCBRACKET FunctionBody RCBRACKET"
        if kids[0].val == 'set':
            self.val = jsast.SetPropertyNode(name=kids[1].val, param=jsast.IDNode(name=kids[3].val), function=kids[6].val)
        else:
            raise SyntaxError("'set' expected")

class PropertyName(Nonterm):
    # IdentifierName | STRING | NUMBER

    def reduce_IdentifierName(self, *kids):
        "%reduce IdentifierName"
        self.val = kids[0].val

    def reduce_STRING(self, *kids):
        "%reduce STRING"
        self.val = jsast.StringLiteralNode(value=kids[0].val)

    def reduce_NUMBER(self, *kids):
        "%reduce NUMBER"
        self.val = jsast.NumericLiteralNode(value=kids[0].val)

class IdentifierName(Nonterm):
    # ID | Keywords

    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = jsast.IDNode(name=kids[0].val)

    def reduce_Keywords(self, *kids):
        "%reduce Keywords"
        self.val = jsast.IDNode(name=kids[0].val)

class KeywordsMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct):
        # create a method for each keyword
        for val, (token, typ) in keywords.js_keywords.items():
            # name and function body
            methodname = 'reduce_%s' % token
            def func(this, *kids):
                #__doc__ = "%%reduce %s" % token
                this.val = kids[0].val
            func.__doc__ = "%%reduce %s" % token
            dct[methodname] = func

        return super().__new__(mcls, name, bases, dct)

class Keywords(Nonterm, metaclass=KeywordsMeta):
    pass

class FunctionBody(Nonterm):
    # !!! just empty now

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = jsast.FunctionBodyNode(body=None)
