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

# [generate]
# { template ; OP ; ... | ... | ... }
# [replace]
# LEFT_TAG -> ProdClass1, RIGHT_TAG -> ProdClass2
class ProductionGenMeta(parsing.NontermMeta):
    def __new__(cls, name, bases, dct):
        # first we need to parse the docstring, which should have 2 parts
        parts = [dct['__doc__'].split('[generate]')[1].split('[replace]')[0].strip()]
        parts += [dct['__doc__'].split('[replace]')[1].strip()]
        # next we parse each part
        # [generate]
        generate_spec = ProductionGenMeta.generate_spec(parts[0])
        # [replace]
        if parts[1] != '':
            replacedoc_spec = [(x[0].strip(), x[1].strip())\
                               for x in [x.split('->') for x in parts[1].split(',')]]
        else:
            replacedoc_spec = []
        # first generate the necessary methods from the template(s)
        ProductionGenMeta.generate_methods(bases, dct, generate_spec)
        # then override all the inherited methods with prefix 'reduce_'
        ProductionGenMeta.override_methods(dct, bases)
        # finally, update docstrings of all the methods with prefix 'reduce_'
        ProductionGenMeta.update_docstrings(dct, replacedoc_spec)
        # bad voodoo if the class' docstring isn't cleaned up
        dct['__doc__'] = ''

#        for i in dct:
#            if i.startswith('reduce_'):
#                print(i, '>>>>', dct[i].__doc__)

        return super().__new__(cls, name, bases, dct)

    @staticmethod
    def generate_spec(text):
        # every item in this spec should ultimately have 3 parts:
        # template_function_name, TERM to be substituted, and a list of values for TERM
        parts = []
        if text != '':
            generate_spec = [x.split(';') for x in text.replace('{', '').split('}')]
            for spec in generate_spec:
                if len(spec) == 3:
                    parts += [(spec[0].strip(), spec[1].strip(), [x.strip() for x in spec[2].split('|')])]
        return parts

    @staticmethod
    def generate_methods(bases, dct, generate_spec):
        for spec in generate_spec:
            template_name = spec[0]
            term = spec[1]
            template_method = ProductionGenMeta.find_super_method(bases, template_name)
            for val in spec[2]:
                m_doc = '%' + template_method.__doc__.replace(term, val)
                m_name = m_doc[1:].replace(' ', '_')
                def wrapper(*args, method=template_method, **kwargs):
                    method(*args, **kwargs)
                dct[m_name] = wrapper
                dct[m_name].__name__ = m_name
                dct[m_name].__doc__ = m_doc

    @staticmethod
    def override_methods(dct, bases):
        # find all the "reduce_" methods in bases
        method_names = {name \
                            for base in bases \
                                for name in base.__dict__ if name.startswith('reduce_')}
        # find the method to copy
        for method_name in method_names:
            method = ProductionGenMeta.find_super_method(bases, method_name)
            # define an overriding method
            def wrapper(*args, method=method, **kwargs):
                method(*args, **kwargs)
            wrapper.__doc__ = method.__doc__
            wrapper.__name__ = method_name
            dct[method_name] = wrapper

    @staticmethod
    def find_super_method(bases, method_name):
        method = None
        for probe in bases:
            try:
                method = getattr(probe, method_name)
            except AttributeError:
                pass
            else:
                break
        assert method
        return method

    @staticmethod
    def update_docstrings(dct, replacedoc_spec):
        # find all the methods prefixed with 'reduce_'
        for method in dct:
            if method.startswith('reduce_'):
                for pair in replacedoc_spec:
                    dct[method].__doc__ = dct[method].__doc__.replace(pair[0], pair[1])


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


class Expression_template():
    # ExpressionSTMT
    # | ExpressionNotSTMT

    def reduce_ExpressionSTMT(self, *kids):
        "%reduce ExpressionSTMT"
        self.val = kids[0].val

    def reduce_ExpressionNotSTMT(self, *kids):
        "%reduce ExpressionNotSTMT"
        self.val = kids[0].val


class PrimaryExpressionNotSTMT(Nonterm):
    # ObjectLiteral

    def reduce_ObjectLiteral(self, *kids):
        "%reduce ObjectLiteral"
        self.val = kids[0].val


class PrimaryExpressionSTMT(Nonterm):
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
        kids[1].val.inparen = True
        self.val = kids[1].val


class ExpressionList_template():
    # AssignmentExpression | ExpressionList COMMA AssignemntExpression

    def reduce_AssignmentExpression(self, *kids):
        "%reduce AssignmentExpressionLEFTMOD"
        self.val = kids[0].val

    def reduce_ExpressionList_COMMA_AssignmentExpression(self, *kids):
        "%reduce ExpressionListLEFTMOD COMMA AssignmentExpressionRIGHTMOD"
        if not isinstance(kids[0].val, jsast.ExpressionListNode):
            kids[0].val = jsast.ExpressionListNode(expressions=[kids[0].val])
        kids[0].val.expressions += [kids[2].val]
        self.val = kids[0].val


class MemberExpressionSTMT(Nonterm):
    # PrimaryExpression
    # | MemberExpression LSBRACKET ExpressionList RSBRACKET
    # | MemberExpression DOT IdentifierName
    # | NEW MemberExpression Arguments

    def reduce_PrimaryExpression(self, *kids):
        "%reduce PrimaryExpressionSTMT"
        self.val = kids[0].val

    def reduce_MemberExpression_LSBRACKET_ExpressionList_RSBRACKET(self, *kids):
        "%reduce MemberExpressionSTMT LSBRACKET ExpressionList RSBRACKET"
        self.val = jsast.SBracketExpressionNode(list=kids[0].val, element=kids[2].val)

    def reduce_MemberExpression_DOT_IdentifierName(self, *kids):
        "%reduce MemberExpressionSTMT DOT IdentifierName"
        self.val = jsast.DotExpressionNode(left=kids[0].val, right=kids[2].val)

    def reduce_NEW_MemberExpression_Arguments(self, *kids):
        "%reduce NEW MemberExpression Arguments"
        self.val = jsast.NewNode(expression=kids[1].val, arguments=kids[2].val)


class MemberExpressionNotSTMT(Nonterm):
    # PrimaryExpression
    # | FunctionExpression
    # | MemberExpression LSBRACKET ExpressionList RSBRACKET
    # | MemberExpression DOT IdentifierName

    def reduce_PrimaryExpression(self, *kids):
        "%reduce PrimaryExpressionNotSTMT"
        self.val = kids[0].val

    def reduce_FunctionExpression(self, *kids):
        "%reduce FunctionExpression"
        self.val = kids[0].val

    def reduce_MemberExpression_LSBRACKET_ExpressionList_RSBRACKET(self, *kids):
        "%reduce MemberExpressionNotSTMT LSBRACKET ExpressionList RSBRACKET"
        self.val = jsast.SBracketExpressionNode(list=kids[0].val, element=kids[2].val)

    def reduce_MemberExpression_DOT_IdentifierName(self, *kids):
        "%reduce MemberExpressionNotSTMT DOT IdentifierName"
        self.val = jsast.DotExpressionNode(left=kids[0].val, right=kids[2].val)


class NewExpressionSTMT(Nonterm):
    # MemberExpression | NEW NewExpression

    def reduce_MemberExpression(self, *kids):
        "%reduce MemberExpressionSTMT"
        self.val = kids[0].val

    def reduce_NEW_NewExpression(self, *kids):
        "%reduce NEW NewExpression"
        self.val = jsast.NewNode(expression=kids[1].val, arguments=None)


class NewExpressionNotSTMT(Nonterm):
    # MemberExpression

    def reduce_MemberExpression(self, *kids):
        "%reduce MemberExpressionNotSTMT"
        self.val = kids[0].val


class CallExpression_template():
    # MemberExpression Arguments
    # | CallExpression Arguments
    # | CallExpression LSBRACKET ExpressionList RSBRACKET
    # | CallExpression DOT IdentifierName

    def reduce_MemberExpression_Arguments(self, *kids):
        "%reduce MemberExpressionLEFTMOD Arguments"
        self.val = jsast.CallNode(call=kids[0].val, arguments=kids[1].val)

    def reduce_CallExpression_Arguments(self, *kids):
        "%reduce CallExpressionLEFTMOD Arguments"
        self.val = jsast.CallNode(call=kids[0].val, arguments=kids[1].val)

    def reduce_CallExpression_LSBRACKET_ExpressionList_RSBRACKET(self, *kids):
        "%reduce CallExpressionLEFTMOD LSBRACKET ExpressionList RSBRACKET"
        self.val = jsast.SBracketExpressionNode(list=kids[0].val, element=kids[2].val)

    def reduce_CallExpression_DOT_IdentifierName(self, *kids):
        "%reduce CallExpressionLEFTMOD DOT IdentifierName"
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

class LHSExpression_template():
    # NewExpression | CallExpression

    def reduce_NewExpression(self, *kids):
        "%reduce NewExpressionLEFTMOD"
        self.val = kids[0].val

    def reduce_CallExpression(self, *kids):
        "%reduce CallExpressionLEFTMOD"
        self.val = kids[0].val

class PostfixExpression_template():
    # !!! line terminator not checked
    #
    # LHSExpression
    # | LHSExpression [no LineTerminator here] PLUSPLUS
    # | LHSExpression [no LineTerminator here] MINUSMINUS

    def reduce_LHSExpression(self, *kids):
        "%reduce LHSExpressionLEFTMOD"
        self.val = kids[0].val

    def reduce_LHSExpression_PLUSPLUS(self, *kids):
        "%reduce LHSExpressionLEFTMOD PLUSPLUS"
        self.val = jsast.PostfixExpressionNode(expression=kids[0].val, op="++")

    def reduce_LHSExpression_MINUSMINUS(self, *kids):
        "%reduce LHSExpressionLEFTMOD MINUSMINUS"
        self.val = jsast.PostfixExpressionNode(expression=kids[0].val, op="--")

class UnaryExpression_template():
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
        "%reduce PostfixExpressionLEFTMOD"
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
class BinaryExpression_template():
    def template(self, *kids):
        'reduce LEFTSIDE OP RIGHTSIDE'
        self.val = jsast.BinExpressionNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)


class MultiplicativeExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; STAR | SLASH | PERCENT }
    [replace] LEFTSIDE -> MultiplicativeExpressionNotSTMT, RIGHTSIDE -> UnaryExpression
    '''
    def reduce_UnaryExpression(self, *kids):
        "%reduce UnaryExpressionNotSTMT"
        self.val = kids[0].val


class MultiplicativeExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; STAR | SLASH | PERCENT }
    [replace] LEFTSIDE -> MultiplicativeExpressionSTMT, RIGHTSIDE -> UnaryExpression
    '''
    def reduce_UnaryExpression(self, *kids):
        "%reduce UnaryExpressionSTMT"
        self.val = kids[0].val


class AdditiveExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; PLUS | MINUS}
    [replace] LEFTSIDE -> AdditiveExpressionNotSTMT, RIGHTSIDE -> MultiplicativeExpression
    '''
    def reduce_MultiplicativeExpression(self, *kids):
        "%reduce MultiplicativeExpressionNotSTMT"
        self.val = kids[0].val


class AdditiveExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; PLUS | MINUS}
    [replace] LEFTSIDE -> AdditiveExpressionSTMT, RIGHTSIDE -> MultiplicativeExpression
    '''
    def reduce_MultiplicativeExpression(self, *kids):
        "%reduce MultiplicativeExpressionSTMT"
        self.val = kids[0].val


class ShiftExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LEFTSHIFT | SIGNRIGHTSHIFT | ZERORIGHTSHIFT}
    [replace] LEFTSIDE -> ShiftExpressionNotSTMT, RIGHTSIDE -> AdditiveExpression
    '''
    def reduce_AdditiveExpression(self, *kids):
        "%reduce AdditiveExpressionNotSTMT"
        self.val = kids[0].val


class ShiftExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LEFTSHIFT | SIGNRIGHTSHIFT | ZERORIGHTSHIFT}
    [replace] LEFTSIDE -> ShiftExpressionSTMT, RIGHTSIDE -> AdditiveExpression
    '''
    def reduce_AdditiveExpression(self, *kids):
        "%reduce AdditiveExpressionSTMT"
        self.val = kids[0].val


class RelationalExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LESS | LESSEQ | GREATER | GREATEREQ}
    [replace] LEFTSIDE -> RelationalExpressionNotSTMT, RIGHTSIDE -> ShiftExpression
    '''
    def reduce_ShiftExpression(self, *kids):
        "%reduce ShiftExpressionNotSTMT"
        self.val = kids[0].val

    def reduce_RelationalExpression_INSTANCEOF_RelationalExpression(self, *kids):
        "%reduce RelationalExpressionNotSTMT INSTANCEOF ShiftExpression"
        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)

    def reduce_RelationalExpression_IN_RelationalExpression(self, *kids):
        "%reduce RelationalExpressionNotSTMT IN ShiftExpression"
        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)


class RelationalExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LESS | LESSEQ | GREATER | GREATEREQ}
    [replace] LEFTSIDE -> RelationalExpressionSTMT, RIGHTSIDE -> ShiftExpression
    '''
    def reduce_ShiftExpression(self, *kids):
        "%reduce ShiftExpressionSTMT"
        self.val = kids[0].val

    def reduce_RelationalExpression_INSTANCEOF_RelationalExpression(self, *kids):
        "%reduce RelationalExpressionSTMT INSTANCEOF ShiftExpression"
        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)

    def reduce_RelationalExpression_IN_RelationalExpression(self, *kids):
        "%reduce RelationalExpressionSTMT IN ShiftExpression"
        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)


#class RelationalExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate] {template; OP; LESS | LESSEQ | GREATER | GREATEREQ}
#    [replace] LEFTSIDE -> RelationalExpressionSTMT, RIGHTSIDE -> ShiftExpression
#    '''
#    def reduce_ShiftExpressionNotSTMT(self, *kids):
#        "%reduce ShiftExpressionNotSTMT"
#        self.val = kids[0].val
#    def reduce_ShiftExpressionSTMT(self, *kids):
#        "%reduce ShiftExpressionSTMT"
#        self.val = kids[0].val
#
#    def reduce_RelationalExpression_INSTANCEOF_RelationalExpression(self, *kids):
#        "%reduce RelationalExpressionNoIN INSTANCEOF RelationalExpressionNoIN"
#        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)


class EqualityExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL}
    [replace] LEFTSIDE -> EqualityExpressionNotSTMT, RIGHTSIDE -> RelationalExpression
    '''
    def reduce_RelationalExpression(self, *kids):
        "%reduce RelationalExpressionNotSTMT"
        self.val = kids[0].val


class EqualityExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL}
    [replace] LEFTSIDE -> EqualityExpressionSTMT, RIGHTSIDE -> RelationalExpression
    '''
    def reduce_RelationalExpression(self, *kids):
        "%reduce RelationalExpressionSTMT"
        self.val = kids[0].val


#class EqualityExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate] {template; OP; EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL}
#    [replace] LEFTSIDE -> EqualityExpressionSTMT, RIGHTSIDE -> RelationalExpression
#    '''
#    def reduce_RelationalExpression(self, *kids):
#        "%reduce RelationalExpressionNoIN"
#        self.val = kids[0].val


class BitwiseANDExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; AND}
    [replace] LEFTSIDE -> BitwiseANDExpressionNotSTMT, RIGHTSIDE -> EqualityExpression
    '''
    def reduce_EqualityExpression(self, *kids):
        "%reduce EqualityExpressionNotSTMT"
        self.val = kids[0].val


class BitwiseANDExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; AND}
    [replace] LEFTSIDE -> BitwiseANDExpressionSTMT, RIGHTSIDE -> EqualityExpression
    '''
    def reduce_EqualityExpression(self, *kids):
        "%reduce EqualityExpressionSTMT"
        self.val = kids[0].val


#class BitwiseANDExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate] {template; OP; AND}
#    [replace] LEFTSIDE -> BitwiseANDExpressionSTMT, RIGHTSIDE -> EqualityExpression
#    '''
#    def reduce_EqualityExpression(self, *kids):
#        "%reduce EqualityExpressionNoIN"
#        self.val = kids[0].val


class BitwiseXORExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; CIRCUM}
    [replace] LEFTSIDE -> BitwiseXORExpressionNotSTMT, RIGHTSIDE -> BitwiseANDExpression
    '''
    def reduce_BitwiseANDExpression(self, *kids):
        "%reduce BitwiseANDExpressionNotSTMT"
        self.val = kids[0].val


class BitwiseXORExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; CIRCUM}
    [replace] LEFTSIDE -> BitwiseXORExpressionSTMT, RIGHTSIDE -> BitwiseANDExpression
    '''
    def reduce_BitwiseANDExpression(self, *kids):
        "%reduce BitwiseANDExpressionSTMT"
        self.val = kids[0].val


#class BitwiseXORExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate] {template; OP; CIRCUM}
#    [replace] LEFTSIDE -> BitwiseXORExpressionSTMT, RIGHTSIDE -> BitwiseANDExpression
#    '''
#    def reduce_BitwiseANDExpression(self, *kids):
#        "%reduce BitwiseANDExpressionNoIN"
#        self.val = kids[0].val


class BitwiseORExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; PIPE}
    [replace] LEFTSIDE -> BitwiseORExpressionNotSTMT, RIGHTSIDE -> BitwiseXORExpression
    '''
    def reduce_BitwiseXORExpression(self, *kids):
        "%reduce BitwiseXORExpressionNotSTMT"
        self.val = kids[0].val


class BitwiseORExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; PIPE}
    [replace] LEFTSIDE -> BitwiseORExpressionSTMT, RIGHTSIDE -> BitwiseXORExpression
    '''
    def reduce_BitwiseXORExpression(self, *kids):
        "%reduce BitwiseXORExpressionSTMT"
        self.val = kids[0].val


#class BitwiseORExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate] {template; OP; PIPE}
#    [replace] LEFTSIDE -> BitwiseORExpressionSTMT, RIGHTSIDE -> BitwiseXORExpression
#    '''
#    def reduce_BitwiseXORExpression(self, *kids):
#        "%reduce BitwiseXORExpressionNoIN"
#        self.val = kids[0].val


class LogicalANDExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LOGICAND}
    [replace] LEFTSIDE -> LogicalANDExpressionNotSTMT, RIGHTSIDE -> BitwiseORExpression
    '''
    def reduce_BitwiseORExpression(self, *kids):
        "%reduce BitwiseORExpressionNotSTMT"
        self.val = kids[0].val


class LogicalANDExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LOGICAND}
    [replace] LEFTSIDE -> LogicalANDExpressionSTMT, RIGHTSIDE -> BitwiseORExpression
    '''
    def reduce_BitwiseORExpression(self, *kids):
        "%reduce BitwiseORExpressionSTMT"
        self.val = kids[0].val


#class LogicalANDExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate] {template; OP; LOGICAND}
#    [replace] LEFTSIDE -> LogicalANDExpressionSTMT, RIGHTSIDE -> BitwiseORExpression
#    '''
#    def reduce_BitwiseORExpression(self, *kids):
#        "%reduce BitwiseORExpressionNoIN"
#        self.val = kids[0].val


class LogicalORExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LOGICOR}
    [replace] LEFTSIDE -> LogicalORExpressionNotSTMT, RIGHTSIDE -> LogicalANDExpression
    '''
    def reduce_LogicalANDExpression(self, *kids):
        "%reduce LogicalANDExpressionNotSTMT"
        self.val = kids[0].val


class LogicalORExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
    '''
    [generate] {template; OP; LOGICOR}
    [replace] LEFTSIDE -> LogicalORExpressionSTMT, RIGHTSIDE -> LogicalANDExpression
    '''
    def reduce_LogicalANDExpression(self, *kids):
        "%reduce LogicalANDExpressionSTMT"
        self.val = kids[0].val


#class LogicalORExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate] {template; OP; LOGICOR}
#    [replace] LEFTSIDE -> LogicalORExpressionSTMT, RIGHTSIDE -> LogicalANDExpression
#    '''
#    def reduce_LogicalANDExpression(self, *kids):
#        "%reduce LogicalANDExpressionNoIN"
#        self.val = kids[0].val


#class BinaryExpressionPreInNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate]
#    {template; OP;
#    PLUS | MINUS | STAR | SLASH | PERCENT
#    | LEFTSHIFT | SIGNRIGHTSHIFT | ZERORIGHTSHIFT }
#    [replace]
#    LEFTSIDE -> BinaryExpressionPreInNotSTMT, RIGHTSIDE -> BinaryExpressionPreIn
#    '''
#    # binary expressions with all of the above ops
#    # also:
#    # UnaryExpression
#
#    def reduce_UnaryExpression(self, *kids):
#        "%reduce UnaryExpressionNotSTMT"
#        self.val = kids[0].val
#
#
#class BinaryExpressionPreInSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate]
#    {template; OP;
#    PLUS | MINUS | STAR | SLASH | PERCENT
#    | LEFTSHIFT | SIGNRIGHTSHIFT | ZERORIGHTSHIFT }
#    [replace]
#    LEFTSIDE -> BinaryExpressionPreInSTMT, RIGHTSIDE -> BinaryExpressionPreIn
#    '''
#    # binary expressions with all of the above ops
#    # also:
#    # UnaryExpression
#
#    def reduce_UnaryExpression(self, *kids):
#        "%reduce UnaryExpressionSTMT"
#        self.val = kids[0].val


#class BinaryExpressionNotSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate]
#    {template; OP;
#    LESS | LESSEQ | GREATER | GREATEREQ
#    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
#    | AND | CIRCUM | PIPE
#    | LOGICAND | LOGICOR }
#    [replace]
#    LEFTSIDE -> BinaryExpressionNotSTMT, RIGHTSIDE -> BinaryExpression
#    '''
#    # binary expressions with all of the above ops
#    # also:
#    # BinaryExpressionPreIn
#    # | BinaryExpression INSTANCEOF BinaryExpression
#    # | BinaryExpression IN BinaryExpression
#    def reduce_BinaryExpressionPreIn(self, *kids):
#        "%reduce BinaryExpressionPreInNotSTMT"
#        self.val = kids[0].val
#
#    def reduce_BinaryExpression_INSTANCEOF_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionNotSTMT INSTANCEOF BinaryExpression"
#        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)
#
#    def reduce_BinaryExpression_IN_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionNotSTMT IN BinaryExpression"
#        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)
#
#
#class BinaryExpressionNoIN(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate]
#    {template; OP;
#    LESS | LESSEQ | GREATER | GREATEREQ
#    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
#    | AND | CIRCUM | PIPE
#    | LOGICAND | LOGICOR }
#    [replace]
#    LEFTSIDE -> BinaryExpressionNoIN, RIGHTSIDE -> BinaryExpressionNoIN
#    '''
#    # binary expressions with all of the above ops
#    # also:
#    # BinaryExpressionPreIn
#    # | BinaryExpression INSTANCEOF BinaryExpression
#    def reduce_BinaryExpressionPreIn(self, *kids):
#        "%reduce BinaryExpressionPreIn"
#        self.val = kids[0].val
#
#    def reduce_BinaryExpressionNoIN_INSTANCEOF_BinaryExpressionNoIN(self, *kids):
#        "%reduce BinaryExpressionNoIN INSTANCEOF BinaryExpressionNoIN"
#        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)
#
#
#class BinaryExpressionSTMT(Nonterm, BinaryExpression_template, metaclass=ProductionGenMeta):
#    '''
#    [generate]
#    {template; OP;
#    LESS | LESSEQ | GREATER | GREATEREQ
#    | EQUAL | NOTEQUAL | STRICTEQUAL | STRICTNOTEQUAL
#    | AND | CIRCUM | PIPE
#    | LOGICAND | LOGICOR }
#    [replace]
#    LEFTSIDE -> BinaryExpressionSTMT, RIGHTSIDE -> BinaryExpressionSTMT
#    '''
#    # binary expressions with all of the above ops
#    # also:
#    # BinaryExpressionPreIn
#    # | BinaryExpression INSTANCEOF BinaryExpression
#    # | BinaryExpression IN BinaryExpression
#    def reduce_BinaryExpressionPreIn(self, *kids):
#        "%reduce BinaryExpressionPreInSTMT"
#        self.val = kids[0].val
#
#    def reduce_BinaryExpression_INSTANCEOF_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionSTMT INSTANCEOF BinaryExpressionSTMT"
#        self.val = jsast.InstanceOfNode(expression=kids[0].val, type=kids[2].val)
#
#    def reduce_BinaryExpression_IN_BinaryExpression(self, *kids):
#        "%reduce BinaryExpressionSTMT IN BinaryExpressionSTMT"
#        self.val = jsast.InNode(expression=kids[0].val, container=kids[2].val)


class ConditionalExpression_template():
    # LogicalORExpression
    # | LogicalORExpression QUESTION AssignmentExpression COLON AssignmentExpression

    def reduce_LogicalORExpression(self, *kids):
        "%reduce LogicalORExpressionLEFTMOD"
        self.val = kids[0].val

    def reduce_BinaryExpression_QUESTION_AssignmentExpression_COLON_AssignmentExpression(self, *kids):
        "%reduce LogicalORExpressionLEFTMOD QUESTION AssignmentExpressionRIGHTMOD COLON AssignmentExpressionRIGHTMOD"
        self.val = jsast.ConditionalExpressionNode(condition=kids[0].val, true=kids[2].val, false=kids[4].val)


class AssignmentExpression_template():
    # ConditionalExpression
    # | LHSExpression AssignmentOp AssignmentExpression

    def reduce_ConditionalExpression(self, *kids):
        "%reduce ConditionalExpressionCONDMOD"
        self.val = kids[0].val

    def reduce_LHSExpression_AssignmentOp_AssignmentExpression(self, *kids):
        "%reduce LHSExpressionLEFTMOD AssignmentOp AssignmentExpressionRIGHTMOD"
        self.val = jsast.AssignmentExpressionNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)


#class AssignmentExpressionNotSTMT(Nonterm):
#    # ConditionalExpression
#    # | LHSExpression AssignmentOp AssignmentExpression
#
#    def reduce_ConditionalExpression(self, *kids):
#        "%reduce BinaryExpressionPreInNotSTMT"
#        self.val = kids[0].val
#
#    def reduce_LHSExpression_AssignmentOp_AssignmentExpression(self, *kids):
#        "%reduce LHSExpressionNotSTMT AssignmentOp AssignmentExpression"
#        self.val = jsast.AssignmentExpressionNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)


# !!!!!

#class PrimaryExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace]
#    ExpressionNotSTMT -> PrimaryExpressionNotSTMT,
#    ExpressionSTMT -> PrimaryExpressionSTMT'''
class ExpressionList(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> ExpressionListNotSTMT,
    ExpressionSTMT -> ExpressionListSTMT'''
class MemberExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> MemberExpressionNotSTMT,
    ExpressionSTMT -> MemberExpressionSTMT'''
class NewExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> NewExpressionNotSTMT,
    ExpressionSTMT -> NewExpressionSTMT'''
#class CallExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace]
#    ExpressionNotSTMT -> CallExpressionNotSTMT,
#    ExpressionSTMT -> CallExpressionSTMT'''
class LHSExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> LHSExpressionNotSTMT,
    ExpressionSTMT -> LHSExpressionSTMT'''
#class PostfixExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace]
#    ExpressionNotSTMT -> PostfixExpressionNotSTMT,
#    ExpressionSTMT -> PostfixExpressionSTMT'''
class UnaryExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> UnaryExpressionNotSTMT,
    ExpressionSTMT -> UnaryExpressionSTMT'''
class MultiplicativeExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> MultiplicativeExpressionNotSTMT,
    ExpressionSTMT -> MultiplicativeExpressionSTMT'''
class AdditiveExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> AdditiveExpressionNotSTMT,
    ExpressionSTMT -> AdditiveExpressionSTMT'''
class ShiftExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> ShiftExpressionNotSTMT,
    ExpressionSTMT -> ShiftExpressionSTMT'''
class RelationalExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> RelationalExpressionNotSTMT,
    ExpressionSTMT -> RelationalExpressionSTMT'''
class EqualityExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> EqualityExpressionNotSTMT,
    ExpressionSTMT -> EqualityExpressionSTMT'''
class BitwiseANDExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> BitwiseANDExpressionNotSTMT,
    ExpressionSTMT -> BitwiseANDExpressionSTMT'''
class BitwiseXORExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> BitwiseXORExpressionNotSTMT,
    ExpressionSTMT -> BitwiseXORExpressionSTMT'''
class BitwiseORExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> BitwiseORExpressionNotSTMT,
    ExpressionSTMT -> BitwiseORExpressionSTMT'''
class LogicalANDExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> LogicalANDExpressionNotSTMT,
    ExpressionSTMT -> LogicalANDExpressionSTMT'''
#class LogicalORExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace]
#    ExpressionNotSTMT -> LogicalORExpressionNotSTMT,
#    ExpressionSTMT -> LogicalORExpressionSTMT'''

#class BinaryExpressionPreIn(Nonterm, Expression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace]
#    ExpressionNotSTMT -> BinaryExpressionPreInNotSTMT,
#    ExpressionSTMT -> BinaryExpressionPreInSTMT'''
#class BinaryExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace]
#    ExpressionNotSTMT -> BinaryExpressionNotSTMT,
#    ExpressionSTMT -> BinaryExpressionSTMT'''
#class ConditionalExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace]
#    ExpressionNotSTMT -> ConditionalExpressionNotSTMT,
#    ExpressionSTMT -> ConditionalExpressionSTMT'''
class AssignmentExpression(Nonterm, Expression_template, metaclass=ProductionGenMeta):
    '''[generate][replace]
    ExpressionNotSTMT -> AssignmentExpressionNotSTMT,
    ExpressionSTMT -> AssignmentExpressionSTMT'''


class ExpressionListNotSTMT(Nonterm, ExpressionList_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> NotSTMT, RIGHTMOD -> '''
class CallExpressionNotSTMT(Nonterm, CallExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> NotSTMT'''
class LHSExpressionNotSTMT(Nonterm, LHSExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> NotSTMT'''
class PostfixExpressionNotSTMT(Nonterm, PostfixExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> NotSTMT'''
class UnaryExpressionNotSTMT(Nonterm):
    def reduce_PostfixExpression(self, *kids):
        "%reduce PostfixExpressionNotSTMT"
        self.val = kids[0].val
class ConditionalExpressionNotSTMT(Nonterm, ConditionalExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> NotSTMT, RIGHTMOD -> '''
class AssignmentExpressionNotSTMT(Nonterm, AssignmentExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] CONDMOD -> NotSTMT, LEFTMOD -> NotSTMT, RIGHTMOD -> '''


class ExpressionListSTMT(Nonterm, ExpressionList_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> STMT, RIGHTMOD -> '''
class CallExpressionSTMT(Nonterm, CallExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> STMT'''
class LHSExpressionSTMT(Nonterm, LHSExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> STMT'''
class PostfixExpressionSTMT(Nonterm, PostfixExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> STMT'''
class UnaryExpressionSTMT(Nonterm, UnaryExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> STMT'''
class ConditionalExpressionSTMT(Nonterm, ConditionalExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] LEFTMOD -> STMT, RIGHTMOD -> '''
class AssignmentExpressionSTMT(Nonterm, AssignmentExpression_template, metaclass=ProductionGenMeta):
    '''[generate][replace] CONDMOD -> STMT, LEFTMOD -> STMT, RIGHTMOD -> '''


#class ExpressionListNoIN(Nonterm, ExpressionList_template, metaclass=ProductionGenMeta):
#    '''[generate][replace] LEFTMOD -> NoIN, RIGHTMOD -> NoIN'''
#class ConditionalExpressionNoIN(Nonterm, ConditionalExpression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace] LEFTMOD -> NoIN, RIGHTMOD -> NoIN'''
#class AssignmentExpressionNoIN(Nonterm, AssignmentExpression_template, metaclass=ProductionGenMeta):
#    '''[generate][replace] CONDMOD -> NoIN, LEFTMOD -> STMT, RIGHTMOD -> NoIN'''


class ExpressionListOPT(Nonterm):
    # <e> | ExpressionList
    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None

    def reduce_ExpressionList(self, *kids):
        "%reduce ExpressionList"
        self.val = kids[0].val


class AssignmentOp_template():
    def template(self, *kids):
        'reduce OP'
        self.val = kids[0].val


class AssignmentOp(Nonterm, AssignmentOp_template, metaclass=ProductionGenMeta):
    '''
    [generate]
    {template; OP;
    ASSIGN | MULTASSIGN | DIVASSIGN | REMAINASSIGN
    | PLUSASSIGN | MINUSASSIGN
    | LSHIFTASSIGN | SRSHIFTASSIGN | ZRSHIFTASSIGN
    | ANDASSIGN | XORASSIGN | ORASSIGN }
    [replace]
    '''


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
        "%reduce ElisionOPT AssignmentExpressionSTMT"
        self.val = kids[0].val + [kids[1].val]

    def reduce_ElementList_COMMA_ElisionOPT_AssignmentExpression(self, *kids):
        "%reduce ElementList COMMA ElisionOPT AssignmentExpressionSTMT"
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
    # function declarations technically must not be here
    # !!! incomplete
    #
    # VariableStatement
    # | Block
    # | SEMICOLON
    # | ExpressionListSTMT SEMICOLON
    # | FunctionExpression
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
        "%reduce ExpressionListSTMT SEMICOLON"
        # need to check if this is actually a function declaration
        self.val = jsast.StatementNode(statement=kids[0].val)

    def reduce_FunctionExpression(self, *kids):
        "%reduce FunctionExpression"
        # need to check if this is actually a function declaration
        if not kids[0].val.name:
            raise SyntaxError("function declaration must include a name")
        else:
            self.val = kids[0].val

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


class OtherStatement_template():
    # various non-if constructs ending with Statement
    #
    # with ( Expression ) EndStatement
    # | Identifier : EndStatement
    # | while ( Expression ) EndStatement
    # | for (ExpressionNoINopt; Expressionopt ; Expressionopt ) EndStatement
    # | for ( var VariableDeclarationListNoIN; Expressionopt ; Expressionopt ) EndStatement for ( LeftHandSideExpression in Expression ) EndStatement
    # | for ( ForInInit ) EndStatement

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
        "%reduce FOR LPAREN ExpressionListOPT SEMICOLON ExpressionListOPT SEMICOLON ExpressionListOPT RPAREN EndStatement"
        if kids[2].val and kids[2].val.countIN() > 0:
            raise SyntaxError("unexpected 'in'")
        else:
            self.val = jsast.ForNode(part1=kids[2].val, part2=kids[4].val, part3=kids[6].val, statement=kids[8].val);

    def reduce_For_with_declaration(self, *kids):
        "%reduce FOR LPAREN VAR VariableDeclarationList SEMICOLON ExpressionListOPT SEMICOLON ExpressionListOPT RPAREN EndStatement"
        declarations = jsast.VarDeclarationNode(vars=kids[3].val)
        if declarations.countIN() > 0:
            raise SyntaxError("unexpected 'in'")
        else:
            self.val = jsast.ForNode(part1=declarations, part2=kids[5].val, part3=kids[7].val, statement=kids[9].val);

    def reduce_ForIn(self, *kids):
        "%reduce FOR LPAREN ForInInit RPAREN EndStatement"
        self.val = jsast.ForInNode(init=kids[2].val[0], array=kids[2].val[1], statement=kids[4].val);


class OtherIfOpenStatement(Nonterm, OtherStatement_template, metaclass=ProductionGenMeta):
    '[generate] [replace] EndStatement -> IfOpenStatement'

class OtherIfClosedStatement(Nonterm, OtherStatement_template, metaclass=ProductionGenMeta):
    '[generate] [replace] EndStatement -> IfClosedStatement'


class VariableStatement(Nonterm):
    # VAR VariableDeclarationList SEMICOLON

    def reduce_VAR_VariableDeclarationList_SEMICOLON(self, *kids):
        "%reduce VAR VariableDeclarationList SEMICOLON"
        self.val = jsast.VarDeclarationNode(vars=kids[1].val)


class VariableDeclarationList_template():
    # VariableDeclaration | VariableDeclarationList COMMA VariableDeclaration

    def reduce_VariableDeclaration(self, *kids):
        "%reduce VariableDeclarationMETAMOD"
        self.val = [kids[0].val]

    def reduce_VariableDeclarationList_COMMA_VariableDeclaration(self, *kids):
        "%reduce VariableDeclarationListMETAMOD COMMA VariableDeclarationMETAMOD"
        self.val = kids[0].val + [kids[2].val]


class VariableDeclaration_template():
    # ID | ID ASSIGN AssignmentExpression

    def reduce_ID(self, *kids):
        "%reduce ID"
        self.val = jsast.VarInitNode(name=kids[0].val, value=None)

    def reduce_ID_ASSIGN_AssignmentExpression(self, *kids):
        "%reduce ID ASSIGN AssignmentExpressionMETAMOD"
        self.val = jsast.VarInitNode(name=kids[0].val, value=kids[2].val)


class VariableDeclarationList(Nonterm, VariableDeclarationList_template, metaclass=ProductionGenMeta):
    '[generate] [replace] METAMOD -> '
class VariableDeclaration(Nonterm, VariableDeclaration_template, metaclass=ProductionGenMeta):
    '[generate] [replace] METAMOD -> '


#class VariableDeclarationNoIN(Nonterm):
#    # just go through the AST and check for presence of 'exposed' in
#    def reduce_VariableDeclaration(self, *kids):
#        "%reduce VariableDeclaration"
#        if kids[0].val.countIN() > 0:
#            raise SyntaxError("unexpected 'in'")
#        else:
#            self.val = kids[0].val
#
#
#class VariableDeclarationListNoIN(Nonterm):
#    # just go through the AST and check for presence of 'exposed' in
#    def reduce_VariableDeclarationList(self, *kids):
#        "%reduce VariableDeclarationList"
#        if kids[0].val.countIN() > 0:
#            raise SyntaxError("unexpected 'in'")
#        else:
#            self.val = kids[0].val


#class VariableDeclarationListNoIN(Nonterm, VariableDeclarationList_template, metaclass=ProductionGenMeta):
#    '[generate] [replace] METAMOD -> NoIN'
#class VariableDeclarationNoIN(Nonterm, VariableDeclaration_template, metaclass=ProductionGenMeta):
#    '[generate] [replace] METAMOD -> NoIN'


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
        "%reduce CASE ` COLON"
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

    def reduce_LCBRACKET_RCBRACKET(self, *kids):
        "%reduce LCBRACKET RCBRACKET"
        self.val = jsast.StatementBlockNode(statements=[])

#    def reduce_LCBRACKET_StatementList_RCBRACKET(self, *kids):
#        "%reduce LCBRACKET BlockStatementList RCBRACKET"
#        self.val = jsast.StatementBlockNode(statements=kids[1].val.code);

    def reduce_LCBRACKET_SourceElements_RCBRACKET(self, *kids):
        "%reduce LCBRACKET SourceElements RCBRACKET"
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
    # LeftHandSideExpression IN ExpressionListSTMT
    # | VAR VariableDeclarationNoIN IN ExpressionListSTMT
    def reduce_LHSExpression(self, *kids):
        "%reduce ExpressionList"
        # needs good magic to split the 'IN' parts
        # as a temp fix, asume the top level op is 'IN'
        self.val = [kids[0].val.expression, kids[0].val.container]

    def reduce_VAR_VariableDeclaration(self, *kids):
        "%reduce VAR VariableDeclaration"
        # needs voodoo to count the INs and split properly
        # as a temp fix, asume the top level op in var declaration is 'IN'
        self.val = [kids[1].val.name, kids[1].val.value.container]


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


#class BlockStatementList(Nonterm):
#    # ; | StatementList Statement
#
##    def reduce_Statement(self, *kids):
##        "%reduce SEMICOLON"
##        self.val = jsast.SourceElementsNode(code=[])
#
#    def reduce_BlStatement(self, *kids):
#        "%reduce ExpressionListSTMT SEMICOLON"
#        self.val = jsast.SourceElementsNode(code=[kids[0].val])
#
##    def reduce_StatementList_Statement(self, *kids):
##        "%reduce BlockStatementList Statement"
##        if kids[1].val != None:
##            kids[0].val.code += [kids[1].val]
##        self.val = kids[0].val

