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

class P_LOGICOR(Precedence, assoc='left', tokens=('LOGICOR',)):
    pass

class P_LOGICAND(Precedence, assoc='left', tokens=('LOGICAND',)):
    pass

class P_BITOR(Precedence, assoc='left', tokens=('PIPE',)):
    pass

class P_BITXOR(Precedence, assoc='left', tokens=('CIRCUM',)):
    pass

class P_BITAND(Precedence, assoc='left', tokens=('AND',)):
    pass

class P_EQUALITY(Precedence, assoc='left' , tokens=('EQUAL', 'NOTEQUAL', 'STRICTEQUAL', 'STRICTNOTEQUAL')):
    pass

class P_RELATIONAL(Precedence, assoc='left', tokens=('LESS', 'LESSEQ', 'GREATER', 'GREATEREQ', 'INSTANCEOF', 'IN')):
    pass

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

class T_REGEXP(Token):
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

class T_GREATER(Token, lextoken='>'):
    pass

class T_LESSEQ(Token, lextoken='<='):
    pass

class T_GREATEREQ(Token, lextoken='>='):
    pass

class T_ASSIGN(Token, lextoken='='):
    pass

class T_BANG(Token, lextoken='!'):
    pass

class T_TILDE(Token, lextoken='~'):
    pass

class T_AND(Token, lextoken='&'):
    pass

class T_PIPE(Token, lextoken='|'):
    pass

class T_LOGICAND(Token, lextoken='&&'):
    pass

class T_LOGICOR(Token, lextoken='||'):
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

class T_EQUAL(Token, lextoken='=='):
    pass

class T_NOTEQUAL(Token, lextoken='!='):
    pass

class T_STRICTEQUAL(Token, lextoken='==='):
    pass

class T_STRICTNOTEQUAL(Token, lextoken='!=='):
    pass

class T_MULTASSIGN(Token, lextoken='*='):
    pass

class T_DIVASSIGN(Token, lextoken='/='):
    pass

class T_REMAINASSIGN(Token, lextoken='%='):
    pass

class T_PLUSASSIGN(Token, lextoken='+='):
    pass

class T_MINUSASSIGN(Token, lextoken='-='):
    pass

class T_LSHIFTASSIGN(Token, lextoken='<<='):
    pass

class T_SRSHIFTASSIGN(Token, lextoken='>>='):
    pass

class T_ZRSHIFTASSIGN(Token, lextoken='>>>='):
    pass

class T_ANDASSIGN(Token, lextoken='&='):
    pass

class T_ORASSIGN(Token, lextoken='|='):
    pass

class T_XORASSIGN(Token, lextoken='^='):
    pass

def _gen_keyword_tokens():
    # Define keyword tokens

    for val, (token, typ) in keywords.js_keywords.items():
        clsname = 'T_%s' % token
        cls = TokenMeta(clsname, (Token,), {'__module__': __name__}, token=token)
        setattr(sys.modules[__name__], clsname, cls)
_gen_keyword_tokens()

# Magic

# Creates a class with a whole bunch of reduce_ClassName_OP_ClassName methods, where
# OP is taken from the class docstring (using '|' as separator)
class BinaryExpressionMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct):
        # create a method for each operation token appearing in docstring
        for token in [x.strip() for x in dct['__doc__'].split('|')]:
            if token != '':
                # name and function body
                methodname = 'reduce_%s_%s_%s' % (name, token, name)
                def func(this, *kids):
                    #__doc__ = '%%reduce %s %s %s' % (name, token, name)
                    this.val = jsast.BinExpressionNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)
                func.__doc__ = '%%reduce %s %s %s' % (name, token, name)
                dct[methodname] = func
        # clean up the docstring, as it apparently affects interpretation
        dct['__doc__'] = ''

        return super().__new__(mcls, name, bases, dct)
class BinaryExpressionGenMeta(type):
    def __new__(mcls, name, bases, dct):
        # create a method for each operation token appearing in docstring
        for token in [x.strip() for x in dct['__doc__'].split('|')]:
            if token != '':
                # name and function body
                methodname = 'reduce_%s_%s_%s' % (name, token, name)
                def func(this, *kids):
                    #__doc__ = '%%reduce %s %s %s' % (name, token, name)
                    this.val = jsast.BinExpressionNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)
                func.__doc__ = '%%reduce %s %s %s' % (name, token, name)
                dct[methodname] = func
        # clean up the docstring, as it apparently affects interpretation
        dct['__doc__'] = ''

        return type.__new__(mcls, name, bases, dct)


# Same as BinaryExpressionMeta except the OP is assumed to be an assignment
class AssignmentOpMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct):
        # create a method for each operation token appearing in docstring
        for token in [x.strip() for x in dct['__doc__'].split('|')]:
            if token != '':
                # name and function body
                methodname = 'reduce_%s' % token
                def func(this, *kids):
                    #__doc__ = '%%reduce %s' % token
                    this.val = kids[0].val
                func.__doc__ = '%%reduce %s' % token
                dct[methodname] = func
        # clean up the docstring, as it apparently affects interpretation
        dct['__doc__'] = ''

        return super().__new__(mcls, name, bases, dct)


# This metaclass will create a class with all the "reduce_" methods overridden.
# The docstring of the overridden methods will be altered by following the rule
# provided in the class docstring: "OldStr1 -> NewStr1, OldStr2 -> NewStr2"
class RenameProductionMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct):
        # figure out what needs to be replaced in the docstring
        findandreplace = [(x[0].strip(), x[1].strip()) for x in [x.split('->') for x in dct['__doc__'].split(',')]]

#        findstr, replacestr = dct['__doc__'].split('->')
#        findstr = findstr.strip()
#        replacestr = replacestr.strip()

        # find all the "reduce_" methods in bases
        method_names = {name \
                            for base in bases \
                                for name in base.__dict__ if name.startswith('reduce_')}
        method_names |= {name for name in dct if name.startswith('reduce_')}

        for method_name in method_names:
            method = None

            # find the method to copy
            for probe in bases:
                try:
                    method = getattr(probe, method_name)
                except AttributeError:
                    pass
                else:
                    break
            assert method

            def wrapper(*args, method=method, **kwargs):
                method(*args, **kwargs)
            # make the altered docstring
            for fnr in findandreplace:
                wrapper.__doc__ = method.__doc__.replace(fnr[0], fnr[1])
            wrapper.__name__ = method_name
            dct[method_name] = wrapper
        # bad voodoo if the class' docstring isn't cleaned up
        dct['__doc__'] = ''
        return super().__new__(mcls, name, bases, dct)


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


# Productions

class Program(Nonterm):
    "%start"

    def reduce_SourceElements(self, *kids):
        "%reduce SourceElements"
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = jsast.SourceElementsNode(code=[])


class SourceElements(Nonterm):
    # SourceElement
    # | SourceElements SourceElement

    def reduce_SourceElement(self, *kids):
        "%reduce SourceElement"
        if kids[0].val == None:
            self.val = jsast.SourceElementsNode(code=[])
        else:
            self.val = jsast.SourceElementsNode(code=[kids[0].val])

    def reduce_SourceElements_SourceElement(self, *kids):
        "%reduce SourceElements SourceElement"
        if kids[1].val != None:
            kids[0].val.code += [kids[1].val]
        self.val = kids[0].val


class SourceElement(Nonterm):
    # Statement | FunctionDeclaration

    def reduce_Statement(self, *kids):
        "%reduce Statement"
        self.val = kids[0].val

#    def reduce_FunctionDeclaration(self, *kids):
#        "%reduce FunctionDeclaration"
#        self.val = kids[0].val


# Expressions

# need to distinguish expresions based on:
# 1) whether they contain 'in'
# 2) whether they start with an object literal


class Literal(Nonterm):
    # NUMBER | STRING | NULL | TRUE | FALSE | REGEXP

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

    def reduce_REGEXP(self, *kids):
        "%reduce REGEXP"
        self.val = jsast.RegExpNode(regexp=kids[0].val)


class PrimaryExpression(Nonterm):
    # PrimaryExpressionNoOBJSTART
    # | ObjectLiteral

    def reduce_PrimaryExpressionNoOBJSTART(self, *kids):
        "%reduce PrimaryExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_ObjectLiteral(self, *kids):
        "%reduce ObjectLiteral"
        self.val = kids[0].val


class PrimaryExpressionNoOBJSTART(Nonterm):
    # this | ID | Literal
    # | ArrayLiteral
    # | LPAREN Expression RPAREN

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

    def reduce_ArrayLiteral(self, *kids):
        "%reduce ArrayLiteral"
        self.val = kids[0].val

    def reduce_LPAREN_ExpressionList_RPAREN(self, *kids):
        "%reduce LPAREN ExpressionList RPAREN"
        self.val = kids[1].val

#    def reduce_LPAREN_ExpressionListNoOBJSTART_RPAREN(self, *kids):
#        "%reduce LPAREN ExpressionList RPAREN"
#        self.val = kids[1].val


class ExpressionList_stub():
    # AssignmentExpression | ExpressionList COMMA AssignemntExpression

    def reduce_AssignmentExpression(self, *kids):
        "%reduce AssignmentExpressionMetaMOD"
        self.val = kids[0].val

    def reduce_ExpressionList_COMMA_AssignmentExpression(self, *kids):
        "%reduce ExpressionListMetaMOD COMMA AssignmentExpressionMetaMOD"
        if not isinstance(kids[0].val, jsast.ExpressionListNode):
            kids[0].val = jsast.ExpressionListNode(expressions=[kids[0].val])
        kids[0].val.expressions += [kids[2].val]
        self.val = kids[0].val


class MemberExpression(Nonterm):
    # PrimaryExpression
    # | FunctionExpression
    # | MemberExpression LSBRACKET ExpressionList RSBRACKET
    # | MemberExpression DOT IdentifierName
    # | NEW MemberExpression Arguments

    def reduce_PrimaryExpression(self, *kids):
        "%reduce PrimaryExpression"
        self.val = kids[0].val

    def reduce_FunctionExpression(self, *kids):
        "%reduce FunctionExpression"
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
        self.val = jsast.DotExpressionNode(left=kids[0].val, right=kids[2].val)

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

    def reduce_ArgumentList_COMMA_AssignmentExpression(self, *kids):
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


# we're attempting to handle 'in' separately
class BinaryExpressionPreIn(Nonterm, metaclass=BinaryExpressionMeta):
    '''PLUS | MINUS | STAR | SLASH | PERCENT
    | LEFTSHIFT | SIGNRIGHTSHIFT | ZERORIGHTSHIFT'''
    # binary expressions with all of the above ops
    # also:
    # UnaryExpression

    def reduce_UnaryExpression(self, *kids):
        "%reduce UnaryExpression"
        self.val = kids[0].val


#class BinaryExpressionMetaMOD(metaclass=BinaryExpressionGenMeta):
#    '''LESS | LESSEQ | GREATER | GREATEREQ
#    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
#    | AND | CIRCUM | PIPE
#    | LOGICAND | LOGICOR'''
#    # binary expressions with all of the above ops
#    # also:
#    # BinaryExpressionPreIn
#    # | BinaryExpression INSTANCEOF BinaryExpression
#    # | BinaryExpression IN BinaryExpression
#
#    def reduce_BinaryExpressionPreIn(self, *kids):
#        "%reduce BinaryExpressionPreIn"
#        self.val = kids[0].val
#
#    def reduce_BinaryExpression_INSTANCEOF_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionMetaMOD INSTANCEOF BinaryExpressionMetaMOD"
#        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)
#
#    def reduce_BinaryExpression_IN_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionMetaMOD IN BinaryExpressionMetaMOD"
#        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)


class BinaryExpression(Nonterm, metaclass=BinaryExpressionMeta):
    '''LESS | LESSEQ | GREATER | GREATEREQ
    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
    | AND | CIRCUM | PIPE
    | LOGICAND | LOGICOR'''
    # binary expressions with all of the above ops
    # also:
    # BinaryExpressionPreIn
    # | BinaryExpression INSTANCEOF BinaryExpression
    # | BinaryExpression IN BinaryExpression

    def reduce_BinaryExpressionPreIn(self, *kids):
        "%reduce BinaryExpressionPreIn"
        self.val = kids[0].val

    def reduce_BinaryExpression_INSTANCEOF_BinaryExpression(self, *kids):
        "%reduce BinaryExpression INSTANCEOF BinaryExpression"
        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)

    def reduce_BinaryExpression_IN_BinaryExpression(self, *kids):
        "%reduce BinaryExpression IN BinaryExpression"
        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)


class BinaryExpressionNoIN(Nonterm, metaclass=BinaryExpressionMeta):
    '''LESS | LESSEQ | GREATER | GREATEREQ
    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
    | AND | CIRCUM | PIPE
    | LOGICAND | LOGICOR'''
    # binary expressions with all of the above ops
    # also:
    # BinaryExpressionPreIn
    # | BinaryExpression INSTANCEOF BinaryExpression
    # | BinaryExpression IN BinaryExpression

    def reduce_BinaryExpressionPreIn(self, *kids):
        "%reduce BinaryExpressionPreIn"
        self.val = kids[0].val

    def reduce_BinaryExpressionNoIN_INSTANCEOF_BinaryExpressionNoIN(self, *kids):
        "%reduce BinaryExpressionNoIN INSTANCEOF BinaryExpressionNoIN"
        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)

class ConditionalExpression_stub():
    # BinaryExpression
    # | BinaryExpression QUESTION AssignmentExpression COLON AssignmentExpression

    def reduce_BinaryExpression(self, *kids):
        "%reduce BinaryExpressionMetaMOD"
        self.val = kids[0].val

    def reduce_BinaryExpression_QUESTION_AssignmentExpression_COLON_AssignmentExpression(self, *kids):
        "%reduce BinaryExpressionMetaMOD QUESTION AssignmentExpressionMetaMOD COLON AssignmentExpressionMetaMOD"
        self.val = jsast.ConditionalExpressionNode(condition=kids[0].val, true=kids[2].val, false=kids[4].val)


class AssignmentExpression_stub():
    # ConditionalExpression
    # | LHSExpression AssignmentOp AssignmentExpression

    def reduce_ConditionalExpression(self, *kids):
        "%reduce ConditionalExpressionMetaMOD"
        self.val = kids[0].val

    def reduce_LHSExpression_AssignmentOp_AssignmentExpression(self, *kids):
        "%reduce LHSExpression AssignmentOp AssignmentExpressionMetaMOD"
        self.val = jsast.AssignmentExpressionNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)


class ExpressionList(Nonterm, ExpressionList_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> '
#class BinaryExpression(Nonterm, BinaryExpressionMetaMOD, metaclass=RenameProductionMeta):
#    'MetaMOD -> '
class ConditionalExpression(Nonterm, ConditionalExpression_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> '
class AssignmentExpression(Nonterm, AssignmentExpression_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> '


class ExpressionListNoIN(Nonterm, ExpressionList_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> NoIN'
#class BinaryExpressionNoIN(Nonterm, BinaryExpressionMetaMOD, metaclass=RenameProductionMeta):
#    'MetaMOD -> NoIN'
class ConditionalExpressionNoIN(Nonterm, ConditionalExpression_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> NoIN'
class AssignmentExpressionNoIN(Nonterm, AssignmentExpression_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> NoIN'


class AssignmentOp(Nonterm, metaclass=AssignmentOpMeta):
    '''ASSIGN | MULTASSIGN | DIVASSIGN | REMAINASSIGN
    | PLUSASSIGN | MINUSASSIGN
    | LSHIFTASSIGN | SRSHIFTASSIGN | ZRSHIFTASSIGN
    | ANDASSIGN | XORASSIGN | ORASSIGN'''


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
            self.val = jsast.GetPropertyNode(name=kids[1].val, functionbody=kids[5].val)
        else:
            raise SyntaxError("'get' expected")

    def reduce_SetProperty(self, *kids):
        "%reduce ID PropertyName LPAREN ID RPAREN LCBRACKET FunctionBody RCBRACKET"
        if kids[0].val == 'set':
            self.val = jsast.SetPropertyNode(name=kids[1].val, param=jsast.IDNode(name=kids[3].val), functionbody=kids[6].val)
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


class Keywords(Nonterm, metaclass=KeywordsMeta):
    pass

# Statements

class Statement(Nonterm):
    # this is handling of dangling 'else'
    #
    # IfOpenStatement | IfClosedStatement

    def reduce_IfOpenStatement(self, *kids):
        "%reduce IfOpenStatement"
        self.val = kids[0].val

    def reduce_IfClosedStatement(self, *kids):
        "%reduce IfClosedStatement"
        self.val = kids[0].val


class BasicStatement(Nonterm):
    # ExpressionList includes function declarations
    # !!! incomplete
    #
    # VariableStatement
    # | Block
    # | SEMICOLON
    # | ExpressionList SEMICOLON
    # | ContinueStatement
    # | BreakStatement
    # | ReturnStatement
    # | switch ( Expression ) CaseBlock
    # | ThrowStatement
    # | TryStatement
    # | StatementLoop
    # | debugger ;

    def reduce_VariableStatement(self, *kids):
        "%reduce VariableStatement"
        self.val = jsast.StatementNode(statement=kids[0].val)

    def reduce_Block(self, *kids):
        "%reduce Block"
        self.val = kids[0].val

    def reduce_SEMICOLON(self, *kids):
        "%reduce SEMICOLON"
        self.val = None

    def reduce_ExpressionList_SEMICOLON(self, *kids):
        "%reduce ExpressionListNoOBJSTART SEMICOLON"
        # need to check if this is actually a function declaration
        # also need to check if this looks like an object literal,
        # but should really be a block statement
        if isinstance(kids[0].val, jsast.FunctionNode):
            if not kids[0].val.name:
                raise SyntaxError("function declaration must include a name")
            else:
                self.val = kids[0].val
        else:
            self.val = jsast.StatementNode(statement=kids[0].val)

    def reduce_ContinueStatement(self, *kids):
        "%reduce ContinueStatement"
        self.val = kids[0].val

    def reduce_BreakStatement(self, *kids):
        "%reduce BreakStatement"
        self.val = kids[0].val

    def reduce_ReturnStatement(self, *kids):
        "%reduce ReturnStatement"
        self.val = kids[0].val

    def reduce_SwitchStatement(self, *kids):
        "%reduce SWITCH LPAREN ExpressionList RPAREN CaseBlock"
        self.val = jsast.SwitchNode(expression=kids[2].val, cases=kids[4].val)

    def reduce_ThrowStatement(self, *kids):
        "%reduce ThrowStatement"
        self.val = kids[0].val

    def reduce_TryStatement(self, *kids):
        "%reduce TryStatement"
        self.val = kids[0].val

    def reduce_StatementLoop(self, *kids):
        "%reduce StatementLoop"
        self.val = kids[0].val

    def reduce_DEBUGGER_SEMICOLON(self, *kids):
        "%reduce DEBUGGER SEMICOLON"
        self.val = jsast.DebuggerNode()


class IfOpenStatement(Nonterm):
    # 'if' isn't matched with an 'else'
    #
    # IfClause Statement
    # | IfClause IfClosedStatement ELSE IfOpenStatement
    # | OtherIfOpenStatements

    def reduce_IfClause_Statement(self, *kids):
        "%reduce IfClause Statement"
        self.val = jsast.IfNode(ifclause=kids[0].val, thenclause=kids[1].val, elseclause=None)

    def reduce_IfClause_IfClosedStatement_ELSE_IfOpenStatement(self, *kids):
        "%reduce IfClause IfClosedStatement ELSE IfOpenStatement"
        self.val = jsast.IfNode(ifclause=kids[0].val, thenclause=kids[1].val, elseclause=kids[3].val)

    def reduce_OtherIfOpenStatement(self, *kids):
        "%reduce OtherIfOpenStatement"
        self.val = kids[0].val


class IfClosedStatement(Nonterm):
    # all 'if' are paired up with 'else'
    #
    # BasicStatement
    # | IfClause IfClosedStatement ELSE IfClosedStatement
    # | OtherIfClosedStatement

    def reduce_BasicStatement(self, *kids):
        "%reduce BasicStatement"
        self.val = kids[0].val

    def reduce_IfClause_IfClosedStatement_ELSE_IfClosedStatement(self, *kids):
        "%reduce IfClause IfClosedStatement ELSE IfClosedStatement"
        self.val = jsast.IfNode(ifclause=kids[0].val, thenclause=kids[1].val, elseclause=kids[3].val)

    def reduce_OtherIfClosedStatement(self, *kids):
        "%reduce OtherIfClosedStatement"
        self.val = kids[0].val


class OtherStatement_stub():
    # various non-if constructs ending with Statement
    #
    # with ( Expression ) EndStatement
    # | Identifier : EndStatement
    # | while ( Expression ) EndStatement
    # | for (ExpressionNoINopt; Expressionopt ; Expressionopt ) EndStatement
    # | for ( var VariableDeclarationListNoIN; Expressionopt ; Expressionopt ) EndStatement for ( LeftHandSideExpression in Expression ) EndStatement
    # | for ( ForInInit in Expression ) EndStatement

    def reduce_WithEndStatement(self, *kids):
        "%reduce WITH LPAREN ExpressionList RPAREN EndStatement"
        self.val = jsast.WithNode(expression=kids[2].val, statement=kids[4].val)

    def reduce_LabelledEndStatement(self, *kids):
        "%reduce ID COLON EndStatement"
        self.val = jsast.LabelNode(id=kids[0].val, statement=kids[2].val)

    def reduce_WHILE_LPAREN_ExpressionList_RPAREN_EndStatement(self, *kids):
        "%reduce WHILE LPAREN ExpressionList RPAREN EndStatement"
        self.val = jsast.WhileNode(expression=kids[2].val, statement=kids[4].val)

    def reduce_For_without_declaration(self, *kids):
        "%reduce FOR LPAREN ExpressionListNoIN SEMICOLON ExpressionList SEMICOLON ExpressionList RPAREN EndStatement"
        self.val = jsast.ForNode(part1=kids[2].val, part2=kids[4].val, part3=kids[6].val, statement=kids[8].val);

    def reduce_For_with_declaration(self, *kids):
        "%reduce FOR LPAREN VAR VariableDeclarationListNoIN SEMICOLON ExpressionList SEMICOLON ExpressionList RPAREN EndStatement"
        self.val = jsast.ForNode(part1=jsast.VarDeclarationNode(vars=kids[3].val), part2=kids[5].val, part3=kids[7].val, statement=kids[9].val);

    def reduce_ForIn(self, *kids):
        "%reduce FOR LPAREN ForInInit IN ExpressionList RPAREN EndStatement"
        self.val = jsast.ForInNode(init=kids[2].val, array=kids[4].val, statement=kids[6].val);


class OtherIfOpenStatement(Nonterm, OtherStatement_stub, metaclass=RenameProductionMeta):
    'EndStatement -> IfOpenStatement'

class OtherIfClosedStatement(Nonterm, OtherStatement_stub, metaclass=RenameProductionMeta):
    'EndStatement -> IfClosedStatement'


class VariableStatement(Nonterm):
    # VAR VariableDeclarationList SEMICOLON

    def reduce_VAR_VariableDeclarationList_SEMICOLON(self, *kids):
        "%reduce VAR VariableDeclarationList SEMICOLON"
        self.val = jsast.VarDeclarationNode(vars=kids[1].val)


class VariableDeclarationList_stub():
    # VariableDeclaration | VariableDeclarationList COMMA VariableDeclaration

    def reduce_VariableDeclaration(self, *kids):
        "%reduce VariableDeclarationMetaMOD"
        self.val = [kids[0].val]

    def reduce_VariableDeclarationList_COMMA_VariableDeclaration(self, *kids):
        "%reduce VariableDeclarationListMetaMOD COMMA VariableDeclarationMetaMOD"
        self.val = kids[0].val + [kids[2].val]


class VariableDeclaration_stub():
    # ID | ID ASSIGN AssignmentExpression

    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = jsast.VarInitNode(name=kids[0].val, value=None)

    def reduce_ID_ASSIGN_AssignmentExpression(self, *kids):
        "%reduce ID ASSIGN AssignmentExpressionMetaMOD"
        self.val = jsast.VarInitNode(name=kids[0].val, value=kids[2].val)


class VariableDeclarationList(Nonterm, VariableDeclarationList_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> '
class VariableDeclaration(Nonterm, VariableDeclaration_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> '


class VariableDeclarationListNoIN(Nonterm, VariableDeclarationList_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> NoIN'
class VariableDeclarationNoIN(Nonterm, VariableDeclaration_stub, metaclass=RenameProductionMeta):
    'MetaMOD -> NoIN'


class CaseBlock(Nonterm):
    # !!! incomplete
    #
    # { CaseClausesopt }
    # | { CaseClausesopt DefaultClause CaseClausesopt }

    def reduce_LCBRACKET_CaseClausesOPT_RCBRACKET(self, *kids):
        "%reduce LCBRACKET CaseClausesOPT RCBRACKET"
        self.val = jsast.StatementBlockNode(statements=kids[1].val)

    def reduce_LCBRACKET_CaseClausesOPT_DefaultClause_CaseClausesOPT_RCBRACKET(self, *kids):
        "%reduce LCBRACKET CaseClausesOPT DefaultClause CaseClausesOPT RCBRACKET"
        self.val = jsast.StatementBlockNode(statements=kids[1].val + [kids[2].val] + kids[3].val)


class CaseClausesOPT(Nonterm):
    # <e> | CaseClauses

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = []

    def reduce_CaseClauses(self, *kids):
        "%reduce CaseClauses"
        self.val = kids[0].val


class CaseClauses(Nonterm):
    # CaseClause | CaseClauses CaseClause

    def reduce_CaseClause(self, *kids):
        "%reduce CaseClause"
        self.val = [kids[0].val]

    def reduce_CaseClauses_CaseClause(self, *kids):
        "%reduce CaseClauses CaseClause"
        self.val = kids[0].val + [kids[1].val]


class CaseClause(Nonterm):
    # case Expression : StatementListopt

    def reduce_CASE_ExpressionList_COLON(self, *kids):
        "%reduce CASE ExpressionList COLON"
        self.val = jsast.CaseNode(case=kids[1].val, statements=[])

    def reduce_CASE_ExpressionList_COLON_StatementList(self, *kids):
        "%reduce CASE ExpressionList COLON StatementList"
        self.val = jsast.CaseNode(case=kids[1].val, statements=kids[3].val)


class DefaultClause(Nonterm):
    # default : StatementListopt

    def reduce_DEFAULT_COLON(self, *kids):
        "%reduce DEFAULT COLON"
        self.val = jsast.DefaultNode(statements=[])

    def reduce_DEFAULT_COLON_StatementList(self, *kids):
        "%reduce DEFAULT COLON StatementList"
        self.val = jsast.DefaultNode(statements=kids[2].val)


class Block(Nonterm):
    # LCBRACKET RCBRACKET
    # | LCBRACKET StatementList RCBRACKET

#    def reduce_LCBRACKET_RCBRACKET(self, *kids):
#        "%reduce LCBRACKET RCBRACKET"
#        self.val = jsast.StatementBlockNode(statements=[])

    def reduce_LCBRACKET_StatementList_RCBRACKET(self, *kids):
        "%reduce LCBRACKET BlockStatementList RCBRACKET"
        self.val = jsast.StatementBlockNode(statements=kids[1].val.code);


class StatementList(Nonterm):
    # Statement | StatementList Statement

    def reduce_Statement(self, *kids):
        "%reduce Statement"
        if kids[0].val == None:
            self.val = jsast.SourceElementsNode(code=[])
        else:
            self.val = jsast.SourceElementsNode(code=[kids[0].val])

    def reduce_StatementList_Statement(self, *kids):
        "%reduce StatementList Statement"
        if kids[1].val != None:
            kids[0].val.code += [kids[1].val]
        self.val = kids[0].val


class IfClause(Nonterm):
    # IF LPAREN ExpressionList RPAREN

    def reduce_IF_LPAREN_ExpressionList_RPAREN(self, *kids):
        "%reduce IF LPAREN ExpressionList RPAREN"
        self.val = kids[2].val


class StatementLoop(Nonterm):
    # Loops NOT ending with Statement
    #
    # do Statement while(Expression);

    def reduce_DO_Statement_WHILE_LPAREN_ExpressionList_RPAREN_SEMICOLON(self, *kids):
        "%reduce DO Statement WHILE LPAREN ExpressionList RPAREN SEMICOLON"
        self.val = jsast.DoNode(statement=kids[1].val, expression=kids[4].val)


class ForInInit(Nonterm):
    # LeftHandSideExpression | VAR VariableDeclarationNoIN

    def reduce_LHSExpression(self, *kids):
        "%reduce LHSExpression"
        self.val = kids[0].val

    def reduce_VAR_VariableDeclarationNoIN(self, *kids):
        "%reduce VAR VariableDeclarationNoIN"
        self.val = jsast.VarDeclarationNode(vars=[kids[1].val])


class ContinueStatement(Nonterm):
    # !!! not handling newline
    # continue [no LineTerminator here] Identifieropt ;

    def reduce_CONTINUE_SEMICOLON(self, *kids):
        "%reduce CONTINUE SEMICOLON"
        self.val = jsast.ContinueNode(id=None)

    def reduce_CONTINUE_ID_SEMICOLON(self, *kids):
        "%reduce CONTINUE ID SEMICOLON"
        self.val = jsast.ContinueNode(id=kids[1].val)

class BreakStatement(Nonterm):
    # !!! not handling newline
    # break [no LineTerminator here] Identifieropt ;

    def reduce_BREAK_SEMICOLON(self, *kids):
        "%reduce BREAK SEMICOLON"
        self.val = jsast.BreakNode(id=None)

    def reduce_BREAK_ID_SEMICOLON(self, *kids):
        "%reduce BREAK ID SEMICOLON"
        self.val = jsast.BreakNode(id=kids[1].val)


class ReturnStatement(Nonterm):
    # !!! not handling newline
    # return [no LineTerminator here] Expressionopt ;

    def reduce_RETURN_SEMICOLON(self, *kids):
        "%reduce RETURN SEMICOLON"
        self.val = jsast.ReturnNode(expression=None)

    def reduce_RETURN_ExpressionList_SEMICOLON(self, *kids):
        "%reduce RETURN ExpressionList SEMICOLON"
        self.val = jsast.ReturnNode(expression=kids[1].val)


class ThrowStatement(Nonterm):
    # !!! not handling newline
    # throw [no LineTerminator here] Expressionopt ;

    def reduce_THROW_SEMICOLON(self, *kids):
        "%reduce THROW SEMICOLON"
        self.val = jsast.ThrowNode(expression=None)

    def reduce_THROW_ExpressionList_SEMICOLON(self, *kids):
        "%reduce THROW ExpressionList SEMICOLON"
        self.val = jsast.ThrowNode(expression=kids[1].val)

class TryStatement(Nonterm):
    # TRY Block Catch
    # | TRY Block Finally
    # | TRY Block Catch Finally

    def reduce_TRY_Block_Catch(self, *kids):
        "%reduce TRY Block Catch"
        self.val = jsast.TryNode(tryblock=kids[1].val, catchid=kids[2].id, catchblock=kids[2].val, finallyblock=None)

    def reduce_TRY_Block_Finally(self, *kids):
        "%reduce TRY Block Finally"
        self.val = jsast.TryNode(tryblock=kids[1].val, catchid=None, catchblock=None, finallyblock=kids[2].val)

    def reduce_TRY_Block_Catch_Finally(self, *kids):
        "%reduce TRY Block Catch Finally"
        self.val = jsast.TryNode(tryblock=kids[1].val, catchid=kids[2].id, catchblock=kids[2].val, finallyblock=kids[3].val)

class Catch(Nonterm):
    # catch ( Identifier ) Block

    def reduce_CATCH_LPAREN_ID_RPAREN_Block(self, *kids):
        "%reduce CATCH LPAREN ID RPAREN Block"
        self.id, self.val = kids[2].val, kids[4].val

class Finally(Nonterm):
    # finally Block

    def reduce_FINALLY_Block(self, *kids):
        "%reduce FINALLY Block"
        self.val = kids[1].val

# function

class FunctionExpression(Nonterm):
    # function ( ) { FunctionBody }
    # | function Identifier ( ) { FunctionBody }
    # | function ( FormalParameterList ) { FunctionBody }
    # | function Identifier ( FormalParameterList ) { FunctionBody }

    def reduce_function_without_id_and_parameters(self, *kids):
        "%reduce FUNCTION LPAREN RPAREN LCBRACKET FunctionBody RCBRACKET"
        self.val = jsast.FunctionNode(name=None, param=None, body=kids[4].val)

    def reduce_function_with_name_and_without_parameters(self, *kids):
        "%reduce FUNCTION ID LPAREN RPAREN LCBRACKET FunctionBody RCBRACKET"
        self.val = jsast.FunctionNode(name=kids[1].val, param=None, body=kids[5].val)

    def reduce_function_without_name_and_with_parameters(self, *kids):
        "%reduce FUNCTION LPAREN FormalParameterList RPAREN LCBRACKET FunctionBody RCBRACKET"
        self.val = jsast.FunctionNode(name=None, param=kids[2].val, body=kids[5].val)

    def reduce_function_with_name_and_parameters(self, *kids):
        "%reduce FUNCTION ID LPAREN FormalParameterList RPAREN LCBRACKET FunctionBody RCBRACKET"
        self.val = jsast.FunctionNode(name=kids[1].val, param=kids[3].val, body=kids[6].val)


class FormalParameterList(Nonterm):
    # ID | FormalParameterList COMMA ID

    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = [jsast.IDNode(name=kids[0].val)]

    def reduce_FormalParameterList_COMMA_ID(self, *kids):
        "%reduce FormalParameterList COMMA ID"
        self.val = kids[0].val + [jsast.IDNode(name=kids[2].val)]


class FunctionBody(Nonterm):
    # <empty> | SourceElements

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = jsast.SourceElementsNode(code=None)

    def reduce_SourceElements(self, *kids):
        "%reduce SourceElements"
        self.val = kids[0].val


# !!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!


class BlockStatementList(Nonterm):
    # ; | StatementList Statement

    def reduce_Statement(self, *kids):
        "%reduce SEMICOLON"
        self.val = jsast.SourceElementsNode(code=[])

#    def reduce_BlStatement(self, *kids):
#        "%reduce ExpressionListNoOBJSTART SEMICOLON"
#        self.val = jsast.SourceElementsNode(code=[kids[0].val])

    def reduce_StatementList_Statement(self, *kids):
        "%reduce BlockStatementList Statement"
        if kids[1].val != None:
            kids[0].val.code += [kids[1].val]
        self.val = kids[0].val

# !!!!!!!!!!!!!!!!!

#class PrimaryExpression(Nonterm):
#    # PrimaryExpressionNoOBJSTART
#    # | ObjectLiteral
#
#    def reduce_PrimaryExpressionNoOBJSTART(self, *kids):
#        "%reduce PrimaryExpressionNoOBJSTART"
#        self.val = kids[0].val
#
#    def reduce_ObjectLiteral(self, *kids):
#        "%reduce ObjectLiteral"
#        self.val = kids[0].val
#
#
#class PrimaryExpressionNoOBJSTART(Nonterm):
#    # this | ID | Literal
#    # | ArrayLiteral
#    # | LPAREN Expression RPAREN
#
#    # identifiers, literals and 'this'
#    def reduce_ID(self, *kids):
#        "%reduce ID"
#        self.val = jsast.IDNode(name=kids[0].val)
#
#    def reduce_Literal(self, *kids):
#        "%reduce Literal"
#        self.val = kids[0].val
#
#    def reduce_THIS(self, *kids):
#        "%reduce THIS"
#        self.val = jsast.ThisNode()
#
#    def reduce_ArrayLiteral(self, *kids):
#        "%reduce ArrayLiteral"
#        self.val = kids[0].val
#
#    def reduce_LPAREN_ExpressionList_RPAREN(self, *kids):
#        "%reduce LPAREN ExpressionList RPAREN"
#        self.val = kids[1].val
#
##    def reduce_LPAREN_ExpressionListNoOBJSTART_RPAREN(self, *kids):
##        "%reduce LPAREN ExpressionList RPAREN"
##        self.val = kids[1].val


class ExpressionListNoOBJSTART(Nonterm):
    # AssignmentExpression | ExpressionList COMMA AssignemntExpression

    def reduce_AssignmentExpression(self, *kids):
        "%reduce AssignmentExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_ExpressionList_COMMA_AssignmentExpression(self, *kids):
        "%reduce ExpressionListNoOBJSTART COMMA AssignmentExpression"
        if not isinstance(kids[0].val, jsast.ExpressionListNode):
            kids[0].val = jsast.ExpressionListNode(expressions=[kids[0].val])
        kids[0].val.expressions += [kids[2].val]
        self.val = kids[0].val


class MemberExpressionNoOBJSTART(Nonterm):
    # PrimaryExpression
    # | FunctionExpression
    # | MemberExpression LSBRACKET ExpressionList RSBRACKET
    # | MemberExpression DOT IdentifierName
    # | NEW MemberExpression Arguments

    def reduce_PrimaryExpression(self, *kids):
        "%reduce PrimaryExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_FunctionExpression(self, *kids):
        "%reduce FunctionExpression"
        self.val = kids[0].val

    def reduce_MemberExpression_LSBRACKET_ExpressionList_RSBRACKET(self, *kids):
        "%reduce MemberExpressionNoOBJSTART LSBRACKET ExpressionList RSBRACKET"
        self.val = jsast.SBracketExpressionNode(list=kids[0].val, element=kids[2].val)

    def reduce_MemberExpression_DOT_IdentifierName(self, *kids):
        "%reduce MemberExpressionNoOBJSTART DOT IdentifierName"
        self.val = jsast.BinExpressionNode(left=kids[0].val, op=".", right=kids[2].val)

    def reduce_NEW_MemberExpression_Arguments(self, *kids):
        "%reduce NEW MemberExpression Arguments"
        self.val = jsast.NewNode(expression=kids[1].val, arguments=kids[2].val)


class NewExpressionNoOBJSTART(Nonterm):
    # MemberExpression | NEW NewExpression

    def reduce_MemberExpression(self, *kids):
        "%reduce MemberExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_NEW_NewExpression(self, *kids):
        "%reduce NEW NewExpression"
        self.val = jsast.NewNode(expression=kids[1].val, arguments=None)

class CallExpressionNoOBJSTART(Nonterm):
    # MemberExpression Arguments
    # | CallExpression Arguments
    # | CallExpression LSBRACKET ExpressionList RSBRACKET
    # | CallExpression DOT IdentifierName

    def reduce_MemberExpression_Arguments(self, *kids):
        "%reduce MemberExpressionNoOBJSTART Arguments"
        self.val = jsast.CallNode(call=kids[0].val, arguments=kids[1].val)

    def reduce_CallExpression_Arguments(self, *kids):
        "%reduce CallExpressionNoOBJSTART Arguments"
        self.val = jsast.CallNode(call=kids[0].val, arguments=kids[1].val)

    def reduce_CallExpression_LSBRACKET_ExpressionList_RSBRACKET(self, *kids):
        "%reduce CallExpressionNoOBJSTART LSBRACKET ExpressionList RSBRACKET"
        self.val = jsast.SBracketExpressionNode(list=kids[0].val, element=kids[2].val)

    def reduce_CallExpression_DOT_IdentifierName(self, *kids):
        "%reduce CallExpressionNoOBJSTART DOT IdentifierName"
        self.val = jsast.DotExpressionNode(left=kids[0].val, right=kids[2].val)


class LHSExpressionNoOBJSTART(Nonterm):
    # NewExpression | CallExpression

    def reduce_NewExpression(self, *kids):
        "%reduce NewExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_CallExpression(self, *kids):
        "%reduce CallExpressionNoOBJSTART"
        self.val = kids[0].val

class PostfixExpressionNoOBJSTART(Nonterm):
    # !!! line terminator not checked
    #
    # LHSExpression
    # | LHSExpression [no LineTerminator here] PLUSPLUS
    # | LHSExpression [no LineTerminator here] MINUSMINUS

    def reduce_LHSExpression(self, *kids):
        "%reduce LHSExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_LHSExpression_PLUSPLUS(self, *kids):
        "%reduce LHSExpressionNoOBJSTART PLUSPLUS"
        self.val = jsast.PostfixExpressionNode(expression=kids[0].val, op="++")

    def reduce_LHSExpression_MINUSMINUS(self, *kids):
        "%reduce LHSExpressionNoOBJSTART MINUSMINUS"
        self.val = jsast.PostfixExpressionNode(expression=kids[0].val, op="--")

class UnaryExpressionNoOBJSTART(Nonterm):
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
        "%reduce PostfixExpressionNoOBJSTART"
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


# we're attempting to handle 'in' separately
class BinaryExpressionPreInNoOBJSTART(Nonterm, metaclass=BinaryExpressionMeta):
    '''PLUS | MINUS | STAR | SLASH | PERCENT
    | LEFTSHIFT | SIGNRIGHTSHIFT | ZERORIGHTSHIFT'''
    # binary expressions with all of the above ops
    # also:
    # UnaryExpression

    def reduce_UnaryExpression(self, *kids):
        "%reduce UnaryExpressionNoOBJSTART"
        self.val = kids[0].val


#class BinaryExpressionMetaMOD(metaclass=BinaryExpressionGenMeta):
#    '''LESS | LESSEQ | GREATER | GREATEREQ
#    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
#    | AND | CIRCUM | PIPE
#    | LOGICAND | LOGICOR'''
#    # binary expressions with all of the above ops
#    # also:
#    # BinaryExpressionPreIn
#    # | BinaryExpression INSTANCEOF BinaryExpression
#    # | BinaryExpression IN BinaryExpression
#
#    def reduce_BinaryExpressionPreIn(self, *kids):
#        "%reduce BinaryExpressionPreIn"
#        self.val = kids[0].val
#
#    def reduce_BinaryExpression_INSTANCEOF_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionMetaMOD INSTANCEOF BinaryExpressionMetaMOD"
#        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)
#
#    def reduce_BinaryExpression_IN_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionMetaMOD IN BinaryExpressionMetaMOD"
#        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)


class BinaryExpressionNoOBJSTART(Nonterm, metaclass=BinaryExpressionMeta):
    '''LESS | LESSEQ | GREATER | GREATEREQ
    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
    | AND | CIRCUM | PIPE
    | LOGICAND | LOGICOR'''
    # binary expressions with all of the above ops
    # also:
    # BinaryExpressionPreIn
    # | BinaryExpression INSTANCEOF BinaryExpression
    # | BinaryExpression IN BinaryExpression

    def reduce_BinaryExpressionPreIn(self, *kids):
        "%reduce BinaryExpressionPreInNoOBJSTART"
        self.val = kids[0].val

    def reduce_BinaryExpression_INSTANCEOF_BinaryExpression(self, *kids):
        "%reduce BinaryExpressionNoOBJSTART INSTANCEOF BinaryExpression"
        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)

    def reduce_BinaryExpression_IN_BinaryExpression(self, *kids):
        "%reduce BinaryExpressionNoOBJSTART IN BinaryExpression"
        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)


class ConditionalExpressionNoOBJSTART(Nonterm):
    # BinaryExpression
    # | BinaryExpression QUESTION AssignmentExpression COLON AssignmentExpression

    def reduce_BinaryExpression(self, *kids):
        "%reduce BinaryExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_BinaryExpression_QUESTION_AssignmentExpression_COLON_AssignmentExpression(self, *kids):
        "%reduce BinaryExpressionNoOBJSTART QUESTION AssignmentExpression COLON AssignmentExpression"
        self.val = jsast.ConditionalExpressionNode(condition=kids[0].val, true=kids[2].val, false=kids[4].val)


class AssignmentExpressionNoOBJSTART(Nonterm):
    # ConditionalExpression
    # | LHSExpression AssignmentOp AssignmentExpression

    def reduce_ConditionalExpression(self, *kids):
        "%reduce ConditionalExpressionNoOBJSTART"
        self.val = kids[0].val

    def reduce_LHSExpression_AssignmentOp_AssignmentExpression(self, *kids):
        "%reduce LHSExpressionNoOBJSTART AssignmentOp AssignmentExpression"
        self.val = jsast.AssignmentExpressionNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)

