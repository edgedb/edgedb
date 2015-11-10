##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


#
# Based on gram.y from PostgreSQL 9.0
#


import sys


from metamagic.utils import ast, parsing
from .. import ast as pgast

from . import keywords
from . import error


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


class P_SET(Precedence, assoc='nonassoc', tokens=('SET',)):
    pass

class P_UNION_EXCEPT(Precedence, assoc='left', tokens=('UNION', 'EXCEPT')):
    pass

class P_INTERSECT(Precedence, assoc='left', tokens=('INTERSECT',)):
    pass

class P_OR(Precedence, assoc='left', tokens=('OR',)):
    pass

class P_AND(Precedence, assoc='left', tokens=('AND',)):
    pass

class P_NOT(Precedence, assoc='right', tokens=('NOT',)):
    pass

class P_EQUALS(Precedence, assoc='right', tokens=('EQUALS',)):
    pass

class P_ANGBRACKET(Precedence, assoc='nonassoc', tokens=('LANGBRACKET', 'RANGBRACKET')):
    pass

class P_LIKE_ILIKE_SIMILAR(Precedence, assoc='nonassoc', tokens=('LIKE', 'ILIKE', 'SIMILAR')):
    pass

class P_ESCAPE(Precedence, assoc='nonassoc', tokens=('ESCAPE',)):
    pass

class P_OVERLAPS(Precedence, assoc='nonassoc', tokens=('OVERLAPS',)):
    pass

class P_BETWEEN(Precedence, assoc='nonassoc', tokens=('BETWEEN',)):
    pass

class P_IN_P(Precedence, assoc='nonassoc', tokens=('IN_P',)):
    pass

class P_POSTFIXOP(Precedence, assoc='left'):
    pass

class P_UNBOUNDED(Precedence, assoc='nonassoc', tokens=('UNBOUNDED',)):
    pass

class P_IDENT(Precedence, assoc='nonassoc', tokens=('IDENT', 'PARTITION', 'RANGE', 'ROWS',
                                                   'PRECEDING', 'FOLLOWING')):
    pass

class P_OP(Precedence, assoc='left', tokens=('Op', 'OPERATOR')):
    pass

class P_NOTNULL(Precedence, assoc='nonassoc', tokens=('NOTNULL',)):
    pass

class P_ISNULL(Precedence, assoc='nonassoc', tokens=('ISNULL',)):
    pass

class P_IS(Precedence, assoc='nonassoc', tokens=('IS', 'NULL_P', 'TRUE_P', 'FALSE_P', 'UNKNOWN')):
    pass

class P_ADD_OP(Precedence, assoc='left', tokens=('PLUS', 'MINUS')):
    pass

class P_MUL_OP(Precedence, assoc='left', tokens=('STAR', 'SLASH', 'PERCENT')):
    pass

class P_POW_OP(Precedence, assoc='left', tokens=('CIRCUM',)):
    pass

# Unary

class P_AT_ZONE(Precedence, assoc='left', tokens=('AT', 'ZONE')):
    pass


class P_UMINUS(Precedence, assoc='right'):
    pass

class P_BRACKET(Precedence, assoc='left', tokens=('LBRACKET', 'RBRACKET')):
    pass

class P_PAREN(Precedence, assoc='left', tokens=('LPAREN', 'RPAREN')):
    pass

class P_TYPECAST(Precedence, assoc='left', tokens=('TYPECAST',)):
    pass

class P_DOT(Precedence, assoc='left', tokens=('DOT',)):
    pass


class T_DOT(Token, lextoken='.'):
    pass

class T_LBRACKET(Token, lextoken='['):
    pass

class T_RBRACKET(Token, lextoken=']'):
    pass

class T_LPAREN(Token, lextoken='('):
    pass

class T_RPAREN(Token, lextoken=')'):
    pass

class T_COLON(Token, lextoken=':'):
    pass

class T_COMMA(Token, lextoken=','):
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

class T_LANGBRACKET(Token, lextoken='<'):
    pass

class T_RANGBRACKET(Token, lextoken='>'):
    pass

class T_EQUALS(Token, lextoken='='):
    pass

class T_ICONST(Token):
    pass

class T_FCONST(Token):
    pass

class T_SCONST(Token):
    pass

class T_BCONST(Token):
    pass

class T_XCONST(Token):
    pass

class T_IDENT(Token):
    pass

class T_PARAM(Token):
    pass

class T_TYPECAST(Token):
    pass

class T_DOTDOT(Token):
    pass

class T_COLONEQUALS(Token):
    pass

class T_Op(Token):
    pass

def _gen_keyword_tokens():
    # Define keyword tokens

    for val, (token, typ) in keywords.pg_keywords.items():
        clsname = 'T_%s' % token
        cls = TokenMeta(clsname, (Token,), {'__module__': __name__}, token=token)
        setattr(sys.modules[__name__], clsname, cls)
_gen_keyword_tokens()


#############
# Productions

class Result(Nonterm):
    "%start"

    def reduce_constraint(self, expr):
        "%reduce ColConstraintElem"
        self.val = expr.val

    def reduce(self, expr):
        "%reduce a_expr"
        self.val = expr.val


class ColConstraintElem(Nonterm):
    # NOT NULL_P | NULL_P | UNIQUE opt_definition OptConsTableSpace
    # | PRIMARY KEY opt_definition OptConsTableSpace
    # | CHECK '(' a_expr ')'

    def reduce_LPAREN_a_expr_RPAREN(self, *kids):
        "%reduce CHECK LPAREN a_expr RPAREN"
        return kids[2]


class attrs(Nonterm):
    # '.' attr_name | attrs '.' attr_name

    def reduce_DOT_attr_name(self, *kids):
        "%reduce DOT attr_name"
        self.val = [kids[1].val]

    def reduce_attrs_DOT_attr_name(self, *kids):
        "%reduce attrs DOT attr_name"
        self.val = kids[0].val + [kids[2].val]


class param_name(Nonterm):
    def reduce_type_function_name(self, *kids):
        "%reduce type_function_name"
        self.val = kids[0].val


class any_operator(Nonterm):
    # all_Op | ColId '.' any_operator

    def reduce_all_Op(self, *kids):
        "%reduce all_Op"
        self.val = [kids[0].val]

    def reduce_ColId_any_operator(self, *kids):
        "%reduce ColId DOT any_operator"
        self.val = [kids[0].val] + kids[2].val


class columnref(Nonterm):
    # ColId | ColId indirection

    def reduce_ColId(self, *kids):
        "%reduce ColId"
        self.val = pgast.FieldRefNode(field=kids[0].val)

    def reduce_ColId_indirection(self, *kids):
        "%reduce ColId indirection"

        colname = kids[0].val
        fieldref = pgast.FieldRefNode()

        nfields = 0
        for indirection in kids[1].val:
            if isinstance(indirection, pgast.IndexIndirectionNode):
                i = pgast.IndirectionNode()

                if nfields == 0:
                    fieldref.field = colname
                    i.indirection = indirection
                else:
                    i.indirection = kids[1].val[nfields:]
                    fieldref.field = tuple(kids[1].val[:nfields])

                i.expr = fieldref
                self.val = i
                return

            elif isinstance(indirection, pgast.StarIndirectionNode):
                if len(kids[1].val) > nfields + 1:
                    raise error.PgSQLParserError('improper use if "*"')

            nfields += 1

        fieldref.field = colname if nfields == 0 else (colname,) + tuple(kids[1].val)
        self.val = fieldref


class indirection_el(Nonterm):
    def reduce_dot_attr_name(self, *kids):
        "%reduce DOT attr_name"
        self.val = kids[1].val

    def reduce_dot_star(self, *kids):
        "%reduce DOT STAR"
        self.val = pgast.StarIndirectionNode()

    def reduce_index_indirection(self, *kids):
        "%reduce LBRACKET a_expr RBRACKET"
        self.val = pgast.IndexIndirectionNode(upper=kids[1].val)

    def reduce_slice_indirection(self, *kids):
        "%reduce LBRACKET a_expr COLON a_expr RBRACKET"
        self.val = pgast.IndexIndirectionNode(lower=kids[1].val, upper=kids[3].val)


class indirection(Nonterm):
    def reduce_indirection_el(self, *kids):
        "%reduce indirection_el"
        self.val = [kids[0].val]

    def reduce_indirection_list(self, *kids):
        "%reduce indirection indirection_el"
        self.val = kids[0].val + [kids[1].val]


class opt_indirection(Nonterm):
    # <e> | opt_indirection indirection_el

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None

    def reduce_opt_indirection_indirection_el(self, *kids):
        "%reduce opt_indirection indirection_el"
        if kids[0].val:
            self.val = kids[0].val + kids[1].val
        else:
            self.val = kids[1].val


class opt_asymmetric(Nonterm):
    # ASYMMETRIC | <e>

    def reduce_ASYMMETRIC(self, *kids):
        "%reduce ASYMMETRIC"

    def reduce_empty(self, *kids):
        "%reduce <e>"


class Typename(Nonterm):
    # SimpleTypename opt_array_bounds | SETOF SimpleTypename opt_array_bounds
    # | SimpleTypename ARRAY '[' ICONST ']' | SETOF SimpleTypename ARRAY '[' ICONST ']'
    # | SimpleTypename ARRAY | SETOF SimpleTypename ARRAY

    def reduce_SimpleTypename_opt_array_bounds(self, *kids):
        "%reduce SimpleTypename opt_array_bounds"
        self.val = kids[0].val
        self.val.array_bounds = kids[1].val

    def reduce_SETOF_SimpleTypename_opt_array_bounds(self, *kids):
        "%reduce SETOF SimpleTypename opt_array_bounds"
        self.val = kids[1].val
        self.val.array_bounds = kids[2].val
        self.val.setof = True

    def reduce_SimpleTypename_ARRAY_ICONST(self, *kids):
        "%reduce SimpleTypename ARRAY LBRACKET ICONST RBRACKET"
        self.val = kids[0].val
        self.val.array_bounds = [kids[3].val]

    def reduce_SETOF_SimpleTypename_ARRAY_ICONST(self, *kids):
        "%reduce SETOF SimpleTypename ARRAY LBRACKET ICONST RBRACKET"
        self.val = kids[1].val
        self.val.array_bounds = [kids[4].val]
        self.val.setof = True

    def reduce_SimpleTypename_ARRAY(self, *kids):
        "%reduce SimpleTypename ARRAY"
        self.val = kids[0].val
        self.val.array_bounds = [-1]

    def reduce_SETOF_SimpleTypename_ARRAY(self, *kids):
        "%reduce SETOF SimpleTypename ARRAY"
        self.val = kids[1].val
        self.val.array_bounds = [-1]
        self.val.setof = True


class opt_array_bounds(Nonterm):
    # opt_array_bounds '[' ']' | opt_array_bounds '[' ICONST ']' | <e>

    def reduce_opt_array_bounds_(self, *kids):
        "%reduce opt_array_bounds LBRACKET RBRACKET"
        self.val = (kids[0].val or []) + [-1]

    def reduce_opt_array_bounds_ICONST(self, *kids):
        "%reduce opt_array_bounds LBRACKET ICONST RBRACKET"
        self.val = (kids[0].val or []) + [kids[2].val]

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class SimpleTypename(Nonterm):
    # GenericType | Numeric | Bit | Character | ConstDatetime | ConstInterval opt_interval
    # ConstInterval '(' ICONST ')' opt_interval

    def reduce_GenericType(self, *kids):
        "%reduce GenericType"
        self.val = kids[0].val

    def reduce_Numeric(self, *kids):
        "%reduce Numeric"
        self.val = kids[0].val

    def reduce_Bit(self, *kids):
        "%reduce Bit"
        self.val = kids[0].val

    def reduce_Character(self, *kids):
        "%reduce Character"
        self.val = kids[0].val

    def reduce_ConstDatetime(self, *kids):
        "%reduce ConstDatetime"
        self.val = kids[0].val

    def reduce_ConstInterval_opt_interval(self, *kids):
        "%reduce ConstInterval opt_interval"
        self.val = kids[0].val
        self.val.typmods = kids[1].val

    def reduce_ConstInterval_ICONST_opt_interval(self, *kids):
        "%reduce ConstInterval LPAREN ICONST RPAREN opt_interval"
        self.val = kids[0].val
        self.val.typmods = kids[1].val

        if kids[4].val:
            prec = kids[4].val.get('precision')
            if prec is not None:
                raise error.PgSQLParserError('interval precision specified twice')
            kids[4].val['precision'] = kids[2].val
            self.val.typmods = [kids[4].val]
        else:
            self.val.typmods = [{'precision': kids[2].val}]


class ConstTypename(Nonterm):
    # Numeric | ConstBit | ConstCharacter | ConstDatetime
    def reduce_Numeric(self, *kids):
        "%reduce Numeric"
        self.val = kids[0].val

    def reduce_ConstBit(self, *kids):
        "%reduce ConstBit"
        self.val = kids[0].val

    def reduce_ConstCharacter(self, *kids):
        "%reduce ConstCharacter"
        self.val = kids[0].val

    def reduce_ConstDatetime(self, *kids):
        "%reduce ConstDatetime"
        self.val = kids[0].val


class GenericType(Nonterm):
    # type_function_name opt_type_modifiers
    # | type_function_name attrs opt_type_modifiers

    def reduce_type_function_name_opt_type_modifiers(self, *kids):
        "%reduce type_function_name opt_type_modifiers"
        self.val = pgast.TypeNode(name=kids[0].val, typmods=kids[1].val)

    def reduce_type_function_name_attrs_opt_type_modifiers(self, *kids):
        "%reduce type_function_name attrs opt_type_modifiers"
        self.val = pgast.TypeNode(name=[kids[0].val] + kids[1].val, typmods=kids[2].val)


class opt_type_modifiers(Nonterm):
    # opt_type_modifiers: '(' expr_list ')'
    # |

    def reduce_expr_list(self, *kids):
        "%reduce LPAREN expr_list RPAREN"
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class Numeric(Nonterm):
    def reduce_INT_P(self, *kids):
        "%reduce INT_P"
        self.val = pgast.TypeNode(name='int4')

    def reduce_INTEGER(self, *kids):
        "%reduce INTEGER"
        self.val = pgast.TypeNode(name='int4')

    def reduce_SMALLINT(self, *kids):
        "%reduce SMALLINT"
        self.val = pgast.TypeNode(name='int2')

    def reduce_BIGINT(self, *kids):
        "%reduce BIGINT"
        self.val = pgast.TypeNode(name='int8')

    def reduce_REAL(self, *kids):
        "%reduce REAL"
        self.val = pgast.TypeNode(name='float4')

    def reduce_FLOAT_P_opt_float(self, *kids):
        "%reduce FLOAT_P opt_float"
        self.val = kids[1].val

    def reduce_DOUBLE_P_PRECISION(self, *kids):
        "%reduce DOUBLE_P PRECISION"
        self.val = pgast.TypeNode(name='float8')

    def reduce_DECIMAL_P_opt_type_modifiers(self, *kids):
        "%reduce DECIMAL_P opt_type_modifiers"
        self.val = pgast.TypeNode(name='numeric', typmods=kids[1].val)

    def reduce_DEC_opt_type_modifiers(self, *kids):
        "%reduce DEC opt_type_modifiers"
        self.val = pgast.TypeNode(name='numeric', typmods=kids[1].val)

    def reduce_NUMERIC_opt_type_modifiers(self, *kids):
        "%reduce NUMERIC opt_type_modifiers"
        self.val = pgast.TypeNode(name='numeric', typmods=kids[1].val)

    def reduce_BOOLEAN_P(self, *kids):
        "%reduce BOOLEAN_P"
        self.val = pgast.TypeNode(name='bool')


class opt_float(Nonterm):
    def reduce_precision(self, *kids):
        "%reduce LPAREN ICONST RPAREN"
        precision = kids[1].val

        if precision < 1:
            raise error.PgSQLParserError('precision for type float must be at least 1 bit')
        elif precision <= 24:
            self.val = pgast.TypeNode(name='float4')
        elif precision <= 53:
            self.val = pgast.TypeNode(name='float8')
        else:
            raise error.PgSQLParserError('precision for type float must be less than 54 bits')

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = pgast.TypeNode(name='float8')


class Bit(Nonterm):
    # BitWithLength | BitWithoutLength

    def reduce_BitWithLength(self, *kids):
        "%reduce BitWithLength"
        self.val = kids[0].val

    def reduce_BitWithoutLength(self, *kids):
        "%reduce BitWithoutLength"
        self.val = kids[0].val


class ConstBit(Nonterm):
    # BitWithLength | BitWithoutLength

    def reduce_BitWithLength(self, *kids):
        "%reduce BitWithLength"
        self.val = kids[0].val

    def reduce_BitWithoutLength(self, *kids):
        "%reduce BitWithoutLength"
        self.val = kids[0].val
        self.val.mods = None


class BitWithLength(Nonterm):
    # BIT opt_varying '(' expr_list ')'

    def reduce(self, *kids):
        "%reduce BIT opt_varying LPAREN expr_list RPAREN"

        if kids[1].val:
            self.val = pgast.TypeNode(name='varbit')
        else:
            self.val = pgast.TypeNode(name='bit')

        self.val.typmods = kids[3].val


class BitWithoutLength(Nonterm):
    # BIT opt_varying

    def reduce(self, *kids):
        "%reduce BIT opt_varying"

        if kids[1].val:
            self.val = pgast.TypeNode(name='varbit')
        else:
            self.val = pgast.TypeNode(name='bit')
            self.val.typmods = [pgast.ConstantNode(value=1)]


class Character(Nonterm):
    # CharacterWithLength | CharacterWithoutLength

    def reduce_CharacterWithLength(self, *kids):
        "%reduce CharacterWithLength"
        self.val = kids[0].val

    def reduce_CharacterWithoutLength(self, *kids):
        "%reduce CharacterWithoutLength"
        self.val = kids[0].val


class ConstCharacter(Nonterm):
    # CharacterWithLength | CharacterWithoutLength

    def reduce_CharacterWithLength(self, *kids):
        "%reduce CharacterWithLength"
        self.val = kids[0].val

    def reduce_CharacterWithoutLength(self, *kids):
        "%reduce CharacterWithoutLength"
        self.val = kids[0].val
        self.val.mods = None


class CharacterWithLength(Nonterm):
    # character '(' ICONST ')' opt_charset

    def reduce(self, *kids):
        "%reduce character LPAREN ICONST RPAREN opt_charset"

        typname = kids[0].val
        if kids[4].val and kids[4].val != 'sql_text':
            typname += '_' + kids[4].val

        self.val = pgast.TypeNode(name=typname, typmods=[kids[2].val])


class CharacterWithoutLength(Nonterm):
    # character opt_charset

    def reduce(self, *kids):
        "%reduce character opt_charset"

        typname = kids[0].val
        if kids[1].val and kids[1].val != 'sql_text':
            typname += '_' + kids[1].val

        self.val = pgast.TypeNode(name=typname)

        if typname == 'bpchar':
            # CHAR defaults to 1
            self.val.typmods = [1]


class character(Nonterm):
    # CHARACTER opt_varying | CHAR_P opt_varying | VARCHAR | NATIONAL CHARACTER opt_varying
    # | NATIONAL CHAR_P opt_varying | NCHAR opt_varying

    def reduce_CHARACTER_opt_varying(self, *kids):
        "%reduce CHARACTER opt_varying"
        self.val = "varchar" if kids[1].val else "bpchar"

    def reduce_CHAR_P_opt_varying(self, *kids):
        "%reduce CHAR_P opt_varying"
        self.val = "varchar" if kids[1].val else "bpchar"

    def reduce_VARCHAR(self, *kids):
        "%reduce VARCHAR"
        self.val = "varchar"

    def reduce_NATIONAL_CHARACTER_opt_varying(self, *kids):
        "%reduce NATIONAL CHARACTER opt_varying"
        self.val = "varchar" if kids[1].val else "bpchar"

    def reduce_NATIONAL_CHAR_P_opt_varying(self, *kids):
        "%reduce NATIONAL CHAR_P opt_varying"
        self.val = "varchar" if kids[1].val else "bpchar"

    def reduce_NCHAR_opt_varying(self, *kids):
        "%reduce NCHAR opt_varying"
        self.val = "varchar" if kids[1].val else "bpchar"


class opt_varying(Nonterm):
    # VARYING | <e>

    def reduce_VARYING(self, *kids):
        "%reduce VARYING"
        self.val = True

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = False


class opt_charset(Nonterm):
    # CHARACTER SET ColId | <e>

    def reduce_CHARACTER_SET_ColId(self, *kids):
        "%reduce CHARACTER SET ColId"
        self.val = kids[2].val

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class ConstDatetime(Nonterm):
    # TIMESTAMP '(' ICONST ')' opt_timezone | TIMESTAMP opt_timezone
    # TIME '(' ICONST ')' opt_timezone | TIME opt_timezone

    def reduce_TIMESTAMP_ICONST_opt_timezone(self, *kids):
        "%reduce TIMESTAMP LPAREN ICONST RPAREN opt_timezone"
        typname = 'timestamptz' if kids[4].val else 'timestamp'
        self.val = pgast.TypeNode(name=typname, typmods=[kids[2].val])

    def reduce_TIMESTAMP_opt_timezone(self, *kids):
        "%reduce TIMESTAMP opt_timezone"
        typname = 'timestamptz' if kids[1].val else 'timestamp'
        self.val = pgast.TypeNode(name=typname)

    def reduce_TIME_ICONST_opt_timezone(self, *kids):
        "%reduce TIME LPAREN ICONST RPAREN opt_timezone"
        typname = 'timetz' if kids[4].val else 'time'
        self.val = pgast.TypeNode(name=typname, typmods=[kids[2].val])

    def reduce_TIME_opt_timezone(self, *kids):
        "%reduce TIME opt_timezone"
        typname = 'timetz' if kids[1].val else 'time'
        self.val = pgast.TypeNode(name=typname)


class ConstInterval(Nonterm):
    # INTERVAL

    def reduce_INTERVAL(self, *kids):
        "%reduce INTERVAL"
        self.val = pgast.TypeNode(name='interval')


class opt_timezone(Nonterm):
    # WITH TIME ZONE | WITHOUT TIME ZONE | <e>

    def reduce_WITH_TIME_ZONE(self, *kids):
        "%reduce WITH TIME ZONE"
        self.val = True

    def reduce_WITHOUT_TIME_ZONE(self, *kids):
        "%reduce WITHOUT TIME ZONE"
        self.val = False

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class opt_interval(Nonterm):
    # YEAR_P | MONTH_P | DAY_P | HOUR_P | MINUTE_P | interval_second | YEAR_P TO MONTH_P
    # | DAY_P TO HOUR_P | DAY_P TO MINUTE_P | DAY_P TO interval_second
    # | HOUR_P TO MINUTE_P | HOUR_P TO interval_second | MINUTE_P TO interval_second
    # | <e>

    def reduce_YEAR_P(self, *kids):
        "%reduce YEAR_P"
        self.val = {'mask': ('year',)}

    def reduce_MONTH_P(self, *kids):
        "%reduce MONTH_P"
        self.val = {'mask': ('month',)}

    def reduce_DAY_P(self, *kids):
        "%reduce DAY_P"
        self.val = {'mask': ('day',)}

    def reduce_HOUR_P(self, *kids):
        "%reduce HOUR_P"
        self.val = {'mask': ('hour',)}

    def reduce_MINUTE_P(self, *kids):
        "%reduce MINUTE_P"
        self.val = {'mask': ('minute',)}

    def reduce_interval_second(self, *kids):
        "%reduce interval_second"
        self.val = kids[0].val

    def reduce_YEAR_P_TO_MONTH_P(self, *kids):
        "%reduce YEAR_P TO MONTH_P"
        self.val = {'mask': ('year', 'month')}

    def reduce_DAY_P_TO_HOUR_P(self, *kids):
        "%reduce DAY_P TO HOUR_P"
        self.val = {'mask': ('day', 'hour')}

    def reduce_DAY_P_TO_MINUTE_P(self, *kids):
        "%reduce DAY_P TO MINUTE_P"
        self.val = {'mask': ('day', 'hour', 'minute')}

    def reduce_DAY_P_TO_interval_second(self, *kids):
        "%reduce DAY_P TO interval_second"
        sec = kids[2].val
        mask = ('day', 'hour', 'minute') + sec['mask']
        self.val = {'mask': mask}
        prec = sec.get('precision')
        if prec:
            self.val['precision'] = prec

    def reduce_HOUR_P_TO_MINUTE_P(self, *kids):
        "%reduce HOUR_P TO MINUTE_P"
        self.val = {'mask': ('hour', 'minute')}

    def reduce_HOUR_P_TO_interval_second(self, *kids):
        "%reduce HOUR_P TO interval_second"
        sec = kids[2].val
        mask = ('hour', 'minute') + sec['mask']
        self.val = {'mask': mask}
        prec = sec.get('precision')
        if prec:
            self.val['precision'] = prec

    def reduce_MINUTE_P_TO_interval_second(self, *kids):
        "%reduce MINUTE_P TO interval_second"
        sec = kids[2].val
        mask = ('minute') + sec['mask']
        self.val = {'mask': mask}
        prec = sec.get('precision')
        if prec:
            self.val['precision'] = prec

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class interval_second(Nonterm):
    # SECOND_P | SECOND_P '(' ICONST ')'

    def reduce_SECOND_P(self, *kids):
        "%reduce SECOND_P"
        self.val = {'mask': ('second',)}

    def reduce_SECOND_P_ICONST(self, *kids):
        "%reduce SECOND_P LPAREN ICONST RPAREN"
        self.val = {'mask': ('second',), 'precision': kids[2].val}


class a_expr(Nonterm):
    # c_expr | a_expr TYPECAST Typename | a_expr AT TIME ZONE a_expr
    # | '+' a_expr | '-' a_expr | a_expr '+' a_expr | a_expr '-' a_expr
    # | a_expr '*' a_expr | a_expr '/' a_expr | a_expr '%' a_expr
    # | a_expr '^' a_expr | a_expr '<' a_expr | a_expr '>' a_expr
    # | a_expr '=' a_expr | a_expr qual_Op a_expr  %prec Op
    # | qual_Op a_expr %prec Op | a_expr qual_Op %prec POSTFIXOP
    # | a_expr AND a_expr | a_expr OR a_expr | NOT a_expr
    # | a_expr LIKE a_expr | a_expr LIKE a_expr ESCAPE a_expr
    # | a_expr NOT LIKE a_expr | a_expr NOT LIKE a_expr ESCAPE a_expr
    # | a_expr ILIKE a_expr | a_expr ILIKE a_expr ESCAPE a_expr
    # | a_expr NOT ILIKE a_expr | a_expr NOT ILIKE a_expr ESCAPE a_expr
    # | a_expr SIMILAR TO a_expr %prec SIMILAR
    # | a_expr SIMILAR TO a_expr ESCAPE a_expr
    # | a_expr NOT SIMILAR TO a_expr %prec SIMILAR
    # | a_expr NOT SIMILAR TO a_expr ESCAPE a_expr
    # | a_expr IS NULL_P | a_expr ISNULL | a_expr IS NOT NULL_P | a_expr NOTNULL
    # | row OVERLAPS row | a_expr IS TRUE_P | a_expr IS NOT TRUE_P | a_expr IS FALSE_P
    # | a_expr IS NOT FALSE_P | a_expr IS UNKNOWN | a_expr IS NOT UNKNOWN
    # | a_expr IS DISTINCT FROM a_expr %prec IS
    # | a_expr IS NOT DISTINCT FROM a_expr %prec IS
    # | a_expr IS OF '(' type_list ')' %prec IS
    # | a_expr IS NOT OF '(' type_list ')' %prec IS
    # | a_expr BETWEEN opt_asymmetric b_expr AND b_expr %prec BETWEEN
    # | a_expr NOT BETWEEN opt_asymmetric b_expr AND b_expr %prec BETWEEN
    # | a_expr BETWEEN SYMMETRIC b_expr AND b_expr %prec BETWEEN
    # | a_expr NOT BETWEEN SYMMETRIC b_expr AND b_expr %prec BETWEEN
    # | a_expr IN_P in_expr | a_expr NOT IN_P in_expr
    # | a_expr subquery_Op sub_type select_with_parens %prec Op
    # | a_expr subquery_Op sub_type '(' a_expr ')' %prec Op
    # | UNIQUE select_with_parens
    # | a_expr IS DOCUMENT_P %prec IS
    # | a_expr IS NOT DOCUMENT_P %prec IS


    def reduce_c_expr(self, *kids):
        "%reduce c_expr"
        self.val = kids[0].val

    def reduce_a_expr_TYPECAST_Typename(self, *kids):
        "%reduce a_expr TYPECAST Typename"

        if isinstance(kids[0].val, pgast.ConstantNode):
            kids[0].val.type = kids[2].val
            self.val = kids[0].val
        else:
            self.val = pgast.TypeCastNode(expr=kids[0].val, type=kids[2].val)

    def reduce_a_expr_AT_TIME_ZONE_a_expr(self, *kids):
        "%reduce a_expr AT TIME ZONE a_expr"
        self.val = pgast.FunctionCallNode(name='timezone', args=[kids[4], kids[0]])

    def reduce_unary_plus(self, *kids):
        "%reduce PLUS a_expr [P_UMINUS]"
        self.val = pgast.UnaryOpNode(op=ast.ops.UPLUS, operand=kids[1].val)

    def reduce_unary_minus(self, *kids):
        "%reduce MINUS a_expr [P_UMINUS]"
        self.val = pgast.UnaryOpNode(op=ast.ops.UMINUS, operand=kids[1].val)

    def reduce_add(self, *kids):
        "%reduce a_expr PLUS a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.ADD, right=kids[2].val)

    def reduce_sub(self, *kids):
        "%reduce a_expr MINUS a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.SUB, right=kids[2].val)

    def reduce_mul(self, *kids):
        "%reduce a_expr STAR a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.MUL, right=kids[2].val)

    def reduce_div(self, *kids):
        "%reduce a_expr SLASH a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.DIV, right=kids[2].val)

    def reduce_mod(self, *kids):
        "%reduce a_expr PERCENT a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.MOD, right=kids[2].val)

    def reduce_pow(self, *kids):
        "%reduce a_expr CIRCUM a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.POW, right=kids[2].val)

    def reduce_lt(self, *kids):
        "%reduce a_expr LANGBRACKET a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LT, right=kids[2].val)

    def reduce_gt(self, *kids):
        "%reduce a_expr RANGBRACKET a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GT, right=kids[2].val)

    def reduce_equals(self, *kids):
        "%reduce a_expr EQUALS a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.EQ, right=kids[2].val)

    def reduce_a_expr_qual_Op_a_expr(self, *kids):
        "%reduce a_expr qual_Op a_expr [P_OP]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)

    def reduce_qual_Op_a_expr(self, *kids):
        "%reduce qual_Op a_expr [P_OP]"
        self.val = pgast.UnaryOpNode(op=kids[0].val, operand=kids[1].val)

    def reduce_a_expr_qual_Op(self, *kids):
        "%reduce a_expr qual_Op [P_POSTFIXOP]"
        self.val = pgast.PostfixOpNode(op=kids[1].val, operand=kids[0].val)

    def reduce_a_expr_AND_a_expr(self, *kids):
        "%reduce a_expr AND a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.AND, right=kids[2].val)

    def reduce_a_expr_OR_a_expr(self, *kids):
        "%reduce a_expr OR a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.OR, right=kids[2].val)

    def reduce_NOT_a_expr(self, *kids):
        "%reduce NOT a_expr"
        self.val = pgast.UnaryOpNode(op=ast.ops.NOT, operand=kids[1].val)

    def reduce_a_expr_LIKE_a_expr(self, *kids):
        "%reduce a_expr LIKE a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.LIKE, right=kids[2].val)

    def reduce_a_expr_LIKE_a_expr_ESCAPE_a_expr(self, *kids):
        "%reduce a_expr LIKE a_expr ESCAPE a_expr"
        right = pgast.FunctionCallNode(name='like_escape', args=[kids[2].val, kids[4].val])
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.LIKE, right=right)

    def reduce_a_expr_NOT_LIKE_a_expr(self, *kids):
        "%reduce a_expr NOT LIKE a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.NOT_LIKE, right=kids[3].val)

    def reduce_a_expr_NOT_LIKE_a_expr_ESCAPE_a_expr(self, *kids):
        "%reduce a_expr NOT LIKE a_expr ESCAPE a_expr"
        right = pgast.FunctionCallNode(name='like_escape', args=[kids[3].val, kids[5].val])
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.NOT_LIKE, right=right)

    def reduce_a_expr_ILIKE_a_expr(self, *kids):
        "%reduce a_expr ILIKE a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.ILIKE, right=kids[2].val)

    def reduce_a_expr_ILIKE_a_expr_ESCAPE_a_expr(self, *kids):
        "%reduce a_expr ILIKE a_expr ESCAPE a_expr"
        right = pgast.FunctionCallNode(name='like_escape', args=[kids[2].val, kids[4].val])
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.ILIKE, right=right)

    def reduce_a_expr_NOT_ILIKE_a_expr(self, *kids):
        "%reduce a_expr NOT ILIKE a_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.NOT_ILIKE, right=kids[3].val)

    def reduce_a_expr_NOT_ILIKE_a_expr_ESCAPE_a_expr(self, *kids):
        "%reduce a_expr NOT ILIKE a_expr ESCAPE a_expr"
        right = pgast.FunctionCallNode(name='like_escape', args=[kids[3].val, kids[5].val])
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.NOT_ILIKE, right=right)

    def reduce_a_expr_SIMILAR_TO_a_expr(self, *kids):
        "%reduce a_expr SIMILAR TO a_expr [P_LIKE_ILIKE_SIMILAR]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.SIMILAR_TO, right=kids[3].val)

    def reduce_a_expr_SIMILAR_TO_a_expr_ESCAPE_a_expr(self, *kids):
        "%reduce a_expr SIMILAR TO a_expr ESCAPE a_expr"
        right = pgast.FunctionCallNode(name='similar_escape', args=[kids[3].val, kids[5].val])
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.SIMILAR_TO, right=right)

    def reduce_a_expr_NOT_SIMILAR_TO_a_expr(self, *kids):
        "%reduce a_expr NOT SIMILAR TO a_expr [P_LIKE_ILIKE_SIMILAR]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.NOT_SIMILAR_TO, right=kids[4].val)

    def reduce_a_expr_NOT_SIMILAR_TO_a_expr_ESCAPE_a_expr(self, *kids):
        "%reduce a_expr NOT SIMILAR TO a_expr ESCAPE a_expr"
        right = pgast.FunctionCallNode(name='similar_escape', args=[kids[4].val, kids[6].val])
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.NOT_SIMILAR_TO, right=right)

    def reduce_a_expr_IS_NULL_P(self, *kids):
        "%reduce a_expr IS NULL_P"
        right = pgast.ConstantNode(value=None)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS, right=right)

    def reduce_a_expr_ISNULL(self, *kids):
        "%reduce a_expr ISNULL"
        right = pgast.ConstantNode(value=None)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS, right=right)

    def reduce_a_expr_IS_NOT_NULL_P(self, *kids):
        "%reduce a_expr IS NOT NULL_P"
        right = pgast.ConstantNode(value=None)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT, right=right)

    def reduce_a_expr_NOTNULL(self, *kids):
        "%reduce a_expr NOTNULL"
        right = pgast.ConstantNode(value=None)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT, right=right)

    def reduce_row_OVERLAPS_row(self, *kids):
        "%reduce row OVERLAPS row"
        self.val = pgast.FunctionCallNode(name='overlaps', args=[kids[0].val, kids[2].val])

    def reduce_a_expr_IS_TRUE_P(self, *kids):
        "%reduce a_expr IS TRUE_P"
        right = pgast.ConstantNode(value=True)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS, right=right)

    def reduce_a_expr_IS_NOT_TRUE_P(self, *kids):
        "%reduce a_expr IS NOT TRUE_P"
        right = pgast.ConstantNode(value=True)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT, right=right)

    def reduce_a_expr_IS_FALSE_P(self, *kids):
        "%reduce a_expr IS FALSE_P"
        right = pgast.ConstantNode(value=False)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS, right=right)

    def reduce_a_expr_IS_NOT_FALSE_P(self, *kids):
        "%reduce a_expr IS NOT FALSE_P"
        right = pgast.ConstantNode(value=False)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT, right=right)

    def reduce_a_expr_IS_UNKNOWN(self, *kids):
        "%reduce a_expr IS UNKNOWN"
        right = pgast.ConstantNode(value=None)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS, right=right)

    def reduce_a_expr_IS_NOT_UNKNOWN(self, *kids):
        "%reduce a_expr IS NOT UNKNOWN"
        right = pgast.ConstantNode(value=None)
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IS_NOT, right=right)

    def reduce_a_expr_IS_DISTINCT_FROM_a_expr(self, *kids):
        "%reduce a_expr IS DISTINCT FROM a_expr [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_DISTINCT, right=kids[4].val)

    def reduce_a_expr_IS_NOT_DISTINCT_FROM_a_expr(self, *kids):
        "%reduce a_expr IS NOT DISTINCT FROM a_expr [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_NOT_DISTINCT, right=kids[5].val)

    def reduce_a_expr_IS_OF_type_list(self, *kids):
        "%reduce a_expr IS OF LPAREN type_list RPAREN [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_OF, right=kids[4].val)

    def reduce_a_expr_IS_NOT_OF_type_list(self, *kids):
        "%reduce a_expr IS NOT OF LPAREN type_list RPAREN [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_NOT_OF, right=kids[5].val)

    def reduce_a_expr_BETWEEN_opt_asymmetric_b_expr_AND_b_expr(self, *kids):
        "%reduce a_expr BETWEEN opt_asymmetric b_expr AND b_expr [P_BETWEEN]"

        left = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GE, right=kids[3].val)
        right = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LE, right=kids[5].val)
        self.val = pgast.BinOpNode(left=left, op=ast.ops.AND, right=right)

    def reduce_a_expr_NOT_BETWEEN_opt_asymmetric_b_expr_AND_b_expr(self, *kids):
        "%reduce a_expr NOT BETWEEN opt_asymmetric b_expr AND b_expr [P_BETWEEN]"

        left = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LT, right=kids[4].val)
        right = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GT, right=kids[6].val)
        self.val = pgast.BinOpNode(left=left, op=ast.ops.OR, right=right)

    def reduce_a_expr_BETWEEN_SYMMETRIC_b_expr_AND_b_expr(self, *kids):
        "%reduce a_expr BETWEEN SYMMETRIC b_expr AND b_expr [P_BETWEEN]"

        left = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GE, right=kids[3].val)
        right = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LE, right=kids[5].val)
        one = pgast.BinOpNode(left=left, op=ast.ops.AND, right=right)
        left = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GE, right=kids[5].val)
        right = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LE, right=kids[3].val)
        second = pgast.BinOpNode(left=left, op=ast.ops.AND, right=right)
        self.val = pgast.BinOpNode(left=one, op=ast.ops.OR, right=second)

    def reduce_a_expr_NOT_BETWEEN_SYMMETRIC_b_expr_AND_b_expr(self, *kids):
        "%reduce a_expr NOT BETWEEN SYMMETRIC b_expr AND b_expr [P_BETWEEN]"

        left = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LT, right=kids[4].val)
        right = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GT, right=kids[6].val)
        one = pgast.BinOpNode(left=left, op=ast.ops.AND, right=right)
        left = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LT, right=kids[6].val)
        right = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GT, right=kids[4].val)
        second = pgast.BinOpNode(left=left, op=ast.ops.AND, right=right)
        self.val = pgast.BinOpNode(left=one, op=ast.ops.OR, right=second)

    def reduce_a_expr_IN_P_in_expr(self, *kids):
        "%reduce a_expr IN_P in_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.IN, right=kids[2].val)

    def reduce_a_expr_NOT_IN_P_in_expr(self, *kids):
        "%reduce a_expr NOT IN_P in_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.NOT_IN, right=kids[3].val)

    # ... XXX


class b_expr(Nonterm):
    # c_expr | b_expr TYPECAST Typename | b_expr AT TIME ZONE b_expr
    # | '+' b_expr | '-' b_expr | b_expr '+' b_expr | b_expr '-' b_expr
    # | b_expr '*' b_expr | b_expr '/' b_expr | b_expr '%' b_expr
    # | b_expr '^' b_expr | b_expr '<' b_expr | b_expr '>' b_expr
    # | b_expr '=' b_expr | b_expr qual_Op b_expr  %prec Op
    # | qual_Op b_expr %prec Op | b_expr qual_Op %prec POSTFIXOP
    # | b_expr IS DISTINCT FROM b_expr %prec IS
    # | b_expr IS NOT DISTINCT FROM b_expr %prec IS
    # | b_expr IS OF '(' type_list ')' %prec IS
    # | b_expr IS NOT OF '(' type_list ')' %prec IS
    # | b_expr IS DOCUMENT_P %prec IS
    # | b_expr IS NOT DOCUMENT_P %prec IS


    def reduce_c_expr(self, *kids):
        "%reduce c_expr"
        self.val = kids[0].val

    def reduce_b_expr_TYPECAST_Typename(self, *kids):
        "%reduce b_expr TYPECAST Typename"

        if isinstance(kids[0].val, pgast.ConstantNode):
            kids[0].val.type = kids[2].val
            self.val = kids[0].val
        else:
            self.val = pgast.TypeCastNode(expr=kids[0].val, type=kids[2].val)

    def reduce_unary_plus(self, *kids):
        "%reduce PLUS b_expr [P_UMINUS]"
        self.val = pgast.UnaryOpNode(op=ast.ops.UPLUS, operand=kids[1].val)

    def reduce_unary_minus(self, *kids):
        "%reduce MINUS b_expr [P_UMINUS]"
        self.val = pgast.UnaryOpNode(op=ast.ops.UMINUS, operand=kids[1].val)

    def reduce_add(self, *kids):
        "%reduce b_expr PLUS b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.ADD, right=kids[2].val)

    def reduce_sub(self, *kids):
        "%reduce b_expr MINUS b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.SUB, right=kids[2].val)

    def reduce_mul(self, *kids):
        "%reduce b_expr STAR b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.MUL, right=kids[2].val)

    def reduce_div(self, *kids):
        "%reduce b_expr SLASH b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.DIV, right=kids[2].val)

    def reduce_mod(self, *kids):
        "%reduce b_expr PERCENT b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.MOD, right=kids[2].val)

    def reduce_pow(self, *kids):
        "%reduce b_expr CIRCUM b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.POW, right=kids[2].val)

    def reduce_lt(self, *kids):
        "%reduce b_expr LANGBRACKET b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.LT, right=kids[2].val)

    def reduce_gt(self, *kids):
        "%reduce b_expr RANGBRACKET b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.GT, right=kids[2].val)

    def reduce_equals(self, *kids):
        "%reduce b_expr EQUALS b_expr"
        self.val = pgast.BinOpNode(left=kids[0].val, op=ast.ops.EQ, right=kids[2].val)

    def reduce_b_expr_qual_Op_b_expr(self, *kids):
        "%reduce b_expr qual_Op b_expr [P_OP]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=kids[1].val, right=kids[2].val)

    def reduce_qual_Op_b_expr(self, *kids):
        "%reduce qual_Op b_expr [P_OP]"
        self.val = pgast.UnaryOpNode(op=kids[0].val, operand=kids[1].val)

    def reduce_b_expr_qual_Op(self, *kids):
        "%reduce b_expr qual_Op [P_POSTFIXOP]"
        self.val = pgast.PostfixOpNode(op=kids[1].val, operand=kids[0].val)

    def reduce_b_expr_IS_DISTINCT_FROM_b_expr(self, *kids):
        "%reduce b_expr IS DISTINCT FROM b_expr [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_DISTINCT, right=kids[4].val)

    def reduce_b_expr_IS_NOT_DISTINCT_FROM_b_expr(self, *kids):
        "%reduce b_expr IS NOT DISTINCT FROM b_expr [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_NOT_DISTINCT, right=kids[5].val)

    def reduce_b_expr_IS_OF_type_list(self, *kids):
        "%reduce b_expr IS OF LPAREN type_list RPAREN [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_OF, right=kids[4].val)

    def reduce_b_expr_IS_NOT_OF_type_list(self, *kids):
        "%reduce b_expr IS NOT OF LPAREN type_list RPAREN [P_IS]"
        self.val = pgast.BinOpNode(left=kids[0].val, op=pgast.IS_NOT_OF, right=kids[5].val)

    # ... XXX


class c_expr(Nonterm):
    # columnref | AexprConst | PARAM opt_indirection | '(' a_expr ')' opt_indirection
    # | case_expr | func_expr | select_with_parens  %prec UMINUS
    # | EXISTS select_with_parens | ARRAY select_with_parens | ARRAY array_expr
    # | row

    def reduce_columnref(self, *kids):
        "%reduce columnref"
        self.val = kids[0].val

    def reduce_AexprConst(self, *kids):
        "%reduce AexprConst"
        self.val = kids[0].val

    def reduce_PARAM_opt_indirection(self, *kids):
        "%reduce PARAM opt_indirection"
        paramref = pgast.ParamRefNode(param=kids[0].val)

        if kids[1].val:
            self.val = pgast.IndirectionNode(expr=paramref, indirection=kids[1].val)
        else:
            self.val = paramref

    def reduce_a_expr_opt_indirection(self, *kids):
        "%reduce LPAREN a_expr RPAREN opt_indirection"

        if kids[3].val:
            self.val = pgast.IndirectionNode(expr=kids[1].val, indirection=kids[3].val)
        else:
            self.val = kids[1].val

    #def reduce_case_expr(self, *kids):
    #    "%reduce case_expr"

    def reduce_func_expr(self, *kids):
        "%reduce func_expr"
        self.val = kids[0].val

    # ... XXX

    def reduce_ARRAY_array_expr(self, *kids):
        "%reduce ARRAY array_expr"
        self.val = pgast.ArrayNode(elements=kids[1].val)

    def reduce_row(self, *kids):
        "%reduce row"
        self.val = pgast.RowExprNode(args=kids[0].val)


class func_expr(Nonterm):
    # func_name '(' ')' over_clause
    # | func_name '(' func_arg_list ')' over_clause
    # | func_name '(' VARIADIC func_arg_expr ')' over_clause
    # | func_name '(' func_arg_list ',' VARIADIC func_arg_expr ')' over_clause
    # | func_name '(' func_arg_list sort_clause ')' over_clause
    # | func_name '(' ALL func_arg_list opt_sort_clause ')' over_clause
    # | func_name '(' DISTINCT func_arg_list opt_sort_clause ')' over_clause
    # | func_name '(' '*' ')' over_clause
    # | CURRENT_DATE | CURRENT_TIME | CURRENT_TIME '(' ICONST ')'
    # | CURRENT_TIMESTAMP | CURRENT_TIMESTAMP '(' ICONST ')'
    # | LOCALTIME | LOCALTIME '(' ICONST ')' | LOCALTIMESTAMP | LOCALTIMESTAMP '(' ICONST ')'
    # | CURRENT_ROLE | CURRENT_USER | SESSION_USER | USER | CURRENT_CATALOG | CURRENT_SCHEMA
    # | CAST '(' a_expr AS Typename ')'
    # | EXTRACT '(' extract_list ')'
    # | OVERLAY '(' overlay_list ')'
    # | POSITION '(' position_list ')'
    # | SUBSTRING '(' substr_list ')'
    # | TREAT '(' a_expr AS Typename ')'
    # | TRIM '(' BOTH trim_list ')'
    # | TRIM '(' LEADING trim_list ')'
    # | TRIM '(' TRAILING trim_list ')'
    # | TRIM '(' trim_list ')'
    # | NULLIF '(' a_expr ',' a_expr ')'
    # | COALESCE '(' expr_list ')'
    # | GREATEST '(' expr_list ')'
    # | LEAST '(' expr_list ')'

    def reduce_func_name_over_clause(self, *kids):
        "%reduce func_name LPAREN RPAREN over_clause"
        self.val = pgast.FunctionCallNode(name=kids[0].val, over=kids[3].val)

    def reduce_func_name_func_arg_list_over_clause(self, *kids):
        "%reduce func_name LPAREN func_arg_list RPAREN over_clause"
        self.val = pgast.FunctionCallNode(name=kids[0].val, args=kids[2].val, over=kids[4].val)

    # ... XXX

    def reduce_CAST_a_expr_AS_Typename(self, *kids):
        "%reduce CAST LPAREN a_expr AS Typename RPAREN"
        self.val = pgast.TypeCastNode(expr=kids[2].val, type=kids[4].val)

    # ... XXX

    def reduce_COALESCE_expr_list(self, *kids):
        "%reduce COALESCE LPAREN expr_list RPAREN"
        self.val = pgast.FunctionCallNode(name=kids[0].val, args=kids[2].val)


class over_clause(Nonterm):
    # OVER window_specification | OVER ColId | <e>

    #def reduce_OVER_window_specification(self, *kids):
    #    "%reduce OVER window_specification"
    #    self.val = kids[0].val

    #def reduce_OVER_ColId(self, *kids):
    #    "%reduce OVER ColId"
    #    self.val = pgast.WindowDefNode(name=kids[1].val)

    def reduce_empty(self, *kids):
        "%reduce <e>"
        self.val = None


class row(Nonterm):
    # ROW '(' expr_list ')' | ROW '(' ')' | '(' expr_list ',' a_expr ')'

    def reduce_ROW_expr_list(self, *kids):
        "%reduce ROW LPAREN expr_list RPAREN"
        self.val = kids[2].val

    def reduce_ROW(self, *kids):
        "%reduce ROW LPAREN RPAREN"
        self.val = None

    def reduce_expr_list_a_expr(self, *kids):
        "%reduce LPAREN expr_list COMMA a_expr RPAREN"
        self.val = kids[1].val + [kids[3].val]


class all_Op(Nonterm):
    # Op | MathOp

    def reduce_Op(self, *kids):
        "%reduce Op"
        self.val = kids[0].val

    def reduce_MathOp(self, *kids):
        "%reduce MathOp"
        self.val = kids[0].val


class MathOp(Nonterm):
    # '+' '-' '*' '/' '%' '^' '<' '>' '='

    def reduce_PLUS(self, *kids):
        "%reduce PLUS"
        self.val = ast.ops.ADD

    def reduce_MINUS(self, *kids):
        "%reduce MINUS"
        self.val = ast.ops.SUB

    def reduce_STAR(self, *kids):
        "%reduce STAR"
        self.val = ast.ops.MUL

    def reduce_SLASH(self, *kids):
        "%reduce SLASH"
        self.val = ast.ops.DIV

    def reduce_PERCENT(self, *kids):
        "%reduce PERCENT"
        self.val = ast.ops.MOD

    def reduce_CIRCUM(self, *kids):
        "%reduce CIRCUM"
        self.val = ast.ops.POW

    def reduce_LANGBRACKET(self, *kids):
        "%reduce LANGBRACKET"
        self.val = ast.ops.LT

    def reduce_RANGBRACKET(self, *kids):
        "%reduce RANGBRACKET"
        self.val = ast.ops.GT

    def reduce_EQUALS(self, *kids):
        "%reduce EQUALS"
        self.val = ast.ops.EQ


class qual_Op(Nonterm):
    # Op | OPERATOR '(' any_operator ')'

    def reduce_Op(self, *kids):
        "%reduce Op"
        self.val = kids[0].val

    def reduce_OPERATOR_any_operator(self, *kids):
        "%reduce OPERATOR LPAREN any_operator RPAREN"
        self.val = kids[2].val


class qual_all_Op(Nonterm):
    # all_Op | OPERATOR '(' any_operator ')'

    def reduce_all_Op(self, *kids):
        "%reduce all_Op"
        self.val = kids[0].val

    def reduce_OPERATOR_any_operator(self, *kids):
        "%reduce OPERATOR LPAREN any_operator RPAREN"
        self.val = kids[2].val


class expr_list(Nonterm):
    # a_expr | expr_list ',' a_expr

    def reduce_a_expr(self, *kids):
        "%reduce a_expr"
        self.val = [kids[0].val]

    def reduce_expr_list_a_expr(self, *kids):
        "%reduce expr_list COMMA a_expr"
        self.val = kids[0].val + [kids[2].val]


class func_arg_list(Nonterm):
    def reduce_func_arg_expr(self, *kids):
        "%reduce func_arg_expr"
        self.val = [kids[0].val]

    def reduce_func_arg_list(self, *kids):
        "%reduce func_arg_list COMMA func_arg_expr"
        self.val = kids[0].val + [kids[2].val]


class func_arg_expr(Nonterm):
    def reduce_a_expr(self, *kids):
        "%reduce a_expr"
        self.val = kids[0].val

    def reduce_a_expr_as_param_name(self, *kids):
        "%reduce a_expr AS param_name"
        #XXX: param_name is ignored
        self.val = kids[0].val


class type_list(Nonterm):
    # Typename | type_list ',' Typename

    def reduce_Typename(self, *kids):
        "%reduce Typename"
        self.val = [kids[0].val]

    def reduce_type_list_Typename(self, *kids):
        "%reduce type_list COMMA Typename"
        self.val = kids[0].val + [kids[2].val]


class array_expr(Nonterm):
    # '[' expr_list ']' | '[' array_expr_list ']' | | '[' ']'

    def reduce_expr_list(self, *kids):
        "%reduce LBRACKET expr_list RBRACKET"
        self.val = kids[1].val

    def reduce_array_expr_list(self, *kids):
        "%reduce LBRACKET array_expr_list RBRACKET"
        self.val = kids[1].val

    def reduce_empty(self, *kids):
        "%reduce LBRACKET RBRACKET"
        self.val = []


class array_expr_list(Nonterm):
    # array_expr | array_expr_list ',' array_expr

    def reduce_array_expr(self, *kids):
        "%reduce array_expr"
        self.val = [kids[0].val]

    def reduce_type_list_Typename(self, *kids):
        "%reduce array_expr_list COMMA array_expr"
        self.val = kids[0].val + [kids[2].val]


class in_expr(Nonterm):
    # select_with_parens | '(' expr_list ')'

    #def reduce_select_with_parens(self, *kids):
    #    "%reduce select_with_parens"

    def reduce_expr_list(self, *kids):
        "%reduce LPAREN expr_list RPAREN"
        self.val = kids[1].val


class attr_name(Nonterm):
    # ColLabel

    def reduce_ColLabel(self, *kids):
        "%reduce ColLabel"
        self.val = kids[0].val


class func_name(Nonterm):
    def reduce_type_function_name(self, *kids):
        "%reduce type_function_name"
        self.val = kids[0].val

    def reduce_col_id_indirection(self, *kids):
        "%reduce ColId indirection"
        for i in kids[1].val:
            if not isinstance(i, str):
                raise error.PgSQLParserError('invalid syntax')
        self.val = (kids[0].val,) + tuple(kids[1].val)


class AexprConst(Nonterm):
    "%nonterm"

    """ Constant:  ICONST
                   | FCONST
                   | SCONST
                   | BCONST
                   | XCONST
                   | func_name SCONST
                   | func_name '(' func_arg_list ')' SCONST
                   | ConstTypename SCONST
                   | ConstInterval SCONST opt_interval
                   | ConstInterval '(' ICONST ')' SCONST opt_interval
                   | TRUE_P
                   | FALSE_P
                   | NULL_P
    """

    def reduce_ICONST(self, *kids):
        "%reduce ICONST"
        self.val = pgast.ConstantNode(value=kids[0].val)

    def reduce_FCONST(self, *kids):
        "%reduce FCONST"
        self.val = pgast.ConstantNode(value=kids[0].val)

    def reduce_SCONST(self, *kids):
        "%reduce SCONST"
        self.val = pgast.ConstantNode(value=kids[0].val)

    def reduce_BCONST(self, *kids):
        "%reduce BCONST"
        self.val = pgast.ConstantNode(value=kids[0].val)

    def reduce_XCONST(self, *kids):
        "%reduce XCONST"
        self.val = pgast.ConstantNode(value=kids[0].val)

    def reduce_type_const(self, *kids):
        "%reduce func_name SCONST"
        self.val = pgast.TypeCastNode(expr=pgast.ConstantNode(value=kids[1].val),
                                      type=pgast.TypeNode(name=kids[0].val))

    def reduce_type_mods_const(self, *kids):
        "%reduce func_name LPAREN func_arg_list RPAREN SCONST"
        self.val = pgast.TypeCastNode(expr=pgast.ConstantNode(value=kids[4].val),
                                      type=pgast.TypeNode(name=kids[0].val, typmods=kids[2].val))

    def reduce_ConstTypename_const(self, *kids):
        "%reduce ConstTypename SCONST"
        self.val = pgast.TypeCastNode(expr=pgast.ConstantNode(value=kids[1].val), type=kids[0].val)

    def reduce_ConstInterval_SCONST_opt_interval(self, *kids):
        "%reduce ConstInterval SCONST opt_interval"
        typ = kids[0].val
        typ.typmods = kids[2].val
        self.val = pgast.TypeCastNode(expr=pgast.ConstantNode(value=kids[1].val), type=typ)

    def reduce_ConstInterval_ICONST_SCONST_opt_interval(self, *kids):
        "%reduce ConstInterval LPAREN ICONST RPAREN SCONST opt_interval"
        typ = kids[0].val

        if kids[5].val:
            prec = kids[5].val.get('precision')
            if prec is not None:
                raise error.PgSQLParserError('interval precision specified twice')
            kids[5].val['precision'] = kids[2].val
            typ.typmods = [kids[5].val]
        else:
            typ.typmods = [{'precision': kids[2].val}]

        self.val = pgast.TypeCastNode(expr=pgast.ConstantNode(value=kids[4].val), type=typ)

    def reduce_TRUE_P(self, *kids):
        "%reduce TRUE_P"
        self.val = pgast.ConstantNode(value=True)

    def reduce_FALSE_P(self, *kids):
        "%reduce FALSE_P"
        self.val = pgast.ConstantNode(value=False)

    def reduce_NULL_P(self, *kids):
        "%reduce NULL_P"
        self.val = pgast.ConstantNode(value=None)


class ColId(Nonterm):
    def reduce_IDENT(self, *kids):
        "%reduce IDENT"
        self.val = kids[0].val

    def reduce_unreserved_keyword(self, *kids):
        "%reduce unreserved_keyword"
        self.val = kids[0].val

    def reduce_col_name_keyword(self, *kids):
        "%reduce col_name_keyword"
        self.val = kids[0].val


class type_function_name(Nonterm):
    def reduce_IDENT(self, *kids):
        "%reduce IDENT"
        self.val = kids[0].val

    def reduce_unreserved_keyword(self, *kids):
        "%reduce unreserved_keyword"
        self.val = kids[0].val

    def reduce_type_func_name_keyword(self, *kids):
        "%reduce type_func_name_keyword"
        self.val = kids[0].val


class ColLabel(Nonterm):
    def reduce_IDENT(self, *kids):
        "%reduce IDENT"
        self.val = kids[0].val

    def reduce_unreserved_keyword(self, *kids):
        "%reduce unreserved_keyword"
        self.val = kids[0].val

    def reduce_col_name_keyword(self, *kids):
        "%reduce col_name_keyword"
        self.val = kids[0].val

    def reduce_type_func_name_keyword(self, *kids):
        "%reduce type_func_name_keyword"
        self.val = kids[0].val

    def reduce_reserved_keyword(self, *kids):
        "%reduce reserved_keyword"
        self.val = kids[0].val


class KeywordMeta(parsing.NontermMeta):
    def __new__(mcls, name, bases, dct, *, type):
        result = super().__new__(mcls, name, bases, dct)

        assert type in keywords.keyword_types

        for val, token in keywords.by_type[type].items():
            def method(inst, *kids):
                inst.val = kids[0].val
            method.__doc__ = "%%reduce %s" % token
            method.__name__ = 'reduce_%s' % token
            setattr(result, method.__name__, method)

        return result

    def __init__(cls, name, bases, dct, *, type):
        super().__init__(name, bases, dct)


class unreserved_keyword(Nonterm, metaclass=KeywordMeta, type=keywords.UNRESERVED_KEYWORD):
    pass


class col_name_keyword(Nonterm, metaclass=KeywordMeta, type=keywords.COL_NAME_KEYWORD):
    pass


class type_func_name_keyword(Nonterm, metaclass=KeywordMeta, type=keywords.TYPE_FUNC_NAME_KEYWORD):
    pass


class reserved_keyword(Nonterm, metaclass=KeywordMeta, type=keywords.RESERVED_KEYWORD):
    pass
