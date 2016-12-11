##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.server.pgsql import ast as pgast


class IRCompilerFunctionSupport:
    def visit_FunctionCall(self, expr):
        ctx = self.context.current

        result = None
        agg_filter = None
        agg_sort = []

        if expr.aggregate:
            with self.context.new():
                ctx.in_aggregate = True
                ctx.query.aggregates = True
                args = [self.visit(a) for a in expr.args]
                if expr.agg_filter:
                    agg_filter = self.visit(expr.agg_filter)
        else:
            args = [self.visit(a) for a in expr.args]

        if expr.agg_sort:
            for sortexpr in expr.agg_sort:
                _sortexpr = self.visit(sortexpr.expr)
                agg_sort.append(
                    pgast.SortBy(
                        node=_sortexpr, dir=sortexpr.direction,
                        nulls=sortexpr.nones_order))

        partition = []
        if expr.partition:
            for partition_expr in expr.partition:
                _pexpr = self.visit(partition_expr)
                partition.append(_pexpr)

        funcname = expr.name
        if funcname[0] == 'std':
            funcname = funcname[1]

        if funcname == 'if':
            cond = self.visit(expr.args[0])
            pos = self.visit(expr.args[1])
            neg = self.visit(expr.args[2])
            when_expr = pgast.CaseWhen(expr=cond, result=pos)
            result = pgast.CaseExpr(args=[when_expr], default=neg)

        elif funcname == 'join':
            name = ('string_agg',)
            separator, ref = args[:2]
            try:
                ignore_nulls = args[2] and args[2].value
            except IndexError:
                ignore_nulls = False

            if not ignore_nulls:
                array_agg = pgast.FuncCall(
                    name=('array_agg',), args=[ref], agg_order=agg_sort)
                result = pgast.FuncCall(
                    name=('array_to_string',), args=[array_agg, separator])
                result.args.append(pgast.Constant(val=''))
            else:
                args = [ref, separator]

        elif funcname == 'count':
            name = ('count',)

        elif funcname == 'current_time':
            result = pgast.Keyword(name='current_time')

        elif funcname == 'current_datetime':
            result = pgast.Keyword(name='current_timestamp')

        elif funcname == 'uuid_generate_v1mc':
            name = ('edgedb', 'uuid_generate_v1mc')

        elif funcname == 'strlen':
            name = ('char_length',)

        elif funcname == 'lpad':
            name = ('lpad',)
            # lpad expects the second argument to be int, so force cast it
            args[1] = pgast.TypeCast(
                expr=args[1], type=pgast.Type(name='int'))

        elif funcname == 'rpad':
            name = ('rpad',)
            # rpad expects the second argument to be int, so force cast it
            args[1] = pgast.TypeCast(
                expr=args[1], type=pgast.Type(name='int'))

        elif funcname == 'levenshtein':
            name = ('edgedb', 'levenshtein')

        elif funcname == 're_match':
            subq = pgast.SelectQuery()

            flags = pgast.FuncCall(
                name=('coalesce',),
                args=[args[2], pgast.Constant(val='')])

            fargs = [args[1], args[0], flags]
            op = pgast.FuncCall(
                name=('regexp_matches',), args=fargs)
            subq.targets.append(op)

            result = subq

        elif funcname == 'strpos':
            r = pgast.FuncCall(name=('strpos',), args=args)
            result = pgast.BinOp(
                left=r, right=pgast.Constant(val=1),
                op=ast.ops.SUB)

        elif funcname == 'substr':
            name = ('substr',)
            args[1] = pgast.TypeCast(
                expr=args[1], type=pgast.Type(name='int'))
            args[1] = pgast.BinOp(
                left=args[1], right=pgast.Constant(val=1),
                op=ast.ops.ADD)
            if args[2] is not None:
                args[2] = pgast.TypeCast(
                    expr=args[2], type=pgast.Type(name='int'))

        elif isinstance(funcname, tuple):
            assert False, 'unsupported function %s' % (funcname, )

        else:
            name = (funcname,)

        if not result:
            if expr.window:
                window_sort = agg_sort
                agg_sort = None

            result = pgast.FuncCall(
                name=name, args=args,
                agg_order=agg_sort, agg_filter=agg_filter)

            if expr.window:
                result.over = pgast.WindowDef(
                    orderby=window_sort, partition=partition)

        return result
