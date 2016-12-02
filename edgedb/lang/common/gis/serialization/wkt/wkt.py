##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common import parsing

from edgedb.lang.common.gis.proto.errors import GeometryError
from .errors import WKTSyntaxError

############
# Precedence


class PrecedenceMeta(parsing.PrecedenceMeta):
    pass

########
# Tokens


class TokenMeta(parsing.TokenMeta):
    pass


class Token(parsing.Token, metaclass=TokenMeta):
    pass


class T_FCONST(Token):
    pass


class T_SRID(Token):
    pass


class T_LPARENTHESIS(Token):
    pass


class T_RPARENTHESIS(Token):
    pass


class T_COMMA(Token):
    pass


class T_SEMICOLON(Token):
    pass


class T_POINT(Token):
    pass


class T_LINESTRING(Token):
    pass


class T_POLYGON(Token):
    pass


class T_MULTIPOINT(Token):
    pass


class T_MULTILINESTRING(Token):
    pass


class T_MULTIPOLYGON(Token):
    pass


class T_GEOMETRYCOLLECTION(Token):
    pass


class T_CIRCULARSTRING(Token):
    pass


class T_COMPOUNDCURVE(Token):
    pass


class T_CURVEPOLYGON(Token):
    pass


class T_MULTICURVE(Token):
    pass


class T_MULTISURFACE(Token):
    pass


class T_CURVE(Token):
    pass


class T_SURFACE(Token):
    pass


class T_POLYHEDRALSURFACE(Token):
    pass


class T_TIN(Token):
    pass


class T_TRIANGLE(Token):
    pass


class T_EMPTY(Token):
    pass

#############
# Productions


class Nonterm(parsing.Nonterm):
    def make_obj(self, tag, values):
        factory = self.parser.parser_data['factory']
        try:
            z_dimension = tag.val.has_z
            m_dimension = tag.val.has_m

            if z_dimension and m_dimension:
                dimensions = ('x', 'y', 'z', 'm')
            elif z_dimension:
                dimensions = ('x', 'y', 'z', None)
            elif m_dimension:
                dimensions = ('x', 'y', None, 'm')
            else:
                dimensions = ('x', 'y', None, None)

            result = factory.new_node(
                type=tag.val.value, dimensions=dimensions, values=values.val)
        except GeometryError as e:
            context = getattr(tag, 'context', None)
            raise WKTSyntaxError('syntax error', context=context) from e

        return result


class Result(Nonterm):
    """%start"""

    def reduce_Geometry(self, expr):
        self.val = expr.val


class Geometry(Nonterm):
    def reduce_OptSrid_GeometryTaggedText(self, *kids):
        self.val = kids[1].val


class OptSrid(Nonterm):
    def reduce_SRID_SEMICOLON(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = -1


class GeometryTaggedText(Nonterm):
    def reduce_PointTaggedText(self, *kids):
        self.val = kids[0].val

    def reduce_LineStringTaggedText(self, *kids):
        self.val = kids[0].val

    def reduce_PolygonTaggedText(self, *kids):
        self.val = kids[0].val

    # def reduce_MultiPointTaggedText(self, *kids):
    #     "%reduce MultiPointTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_MultiPolygonTaggedText(self, *kids):
    #     "%reduce MultiPolygonTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_GeometryCollectionTaggedText(self, *kids):
    #     "%reduce GeometryCollectionTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_CircularStringTaggedText(self, *kids):
    #     "%reduce CircularStringTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_CompoundCurveTaggedText(self, *kids):
    #     "%reduce CompoundCurveTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_CurvePolygonTaggedText(self, *kids):
    #     "%reduce CurvePolygonTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_MultiCurveTaggedText(self, *kids):
    #     "%reduce MultiCurveTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_MultiSurfaceTaggedText(self, *kids):
    #     "%reduce MultiSurfaceTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_CurveTaggedText(self, *kids):
    #     "%reduce CurveTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_SurfaceTaggedText(self, *kids):
    #     "%reduce SurfaceTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_PolyhedralSurfaceTaggedText(self, *kids):
    #     "%reduce PolyhedralSurfaceTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_TinTaggedText(self, *kids):
    #     "%reduce TinTaggedText"
    #     self.val = kids[0].val
    #
    # def reduce_TriangleTaggedText(self, *kids):
    #     "%reduce TriangleTaggedText"
    #     self.val = kids[0].val


class PointTaggedText(Nonterm):
    def reduce_POINT_PointText(self, *kids):
        self.val = self.make_obj(*kids)


class PointText(Nonterm):
    def reduce_PointInParens(self, *kids):
        self.val = kids[0].val

    def reduce_EMPTY(self, *kids):
        self.val = ()


class PointInParens(Nonterm):
    def reduce_LPARENTHESIS_Point_RPARENTHESIS(self, *kids):
        self.val = kids[1].val


class Point(Nonterm):
    def reduce_FCONST_FCONST(self, *kids):
        self.val = (kids[0].val, kids[1].val)

    def reduce_FCONST_FCONST_FCONST(self, *kids):
        self.val = (kids[0].val, kids[1].val, kids[2].val)

    def reduce_FCONST_FCONST_FCONST_FCONST(self, *kids):
        self.val = (kids[0].val, kids[1].val, kids[2].val, kids[3].val)


class LineStringTaggedText(Nonterm):
    def reduce_LINESTRING_LineStringText(self, *kids):
        self.val = self.make_obj(*kids)


class LineStringText(Nonterm):
    def reduce_LineStringInParens(self, *kids):
        self.val = kids[0].val

    def reduce_EMPTY(self, *kids):
        self.val = ()


class LineStringInParens(Nonterm):
    def reduce_LPARENTHESIS_LineString_RPARENTHESIS(self, *kids):
        self.val = kids[1].val


class LineString(Nonterm):
    def reduce_Point(self, *kids):
        self.val = [kids[0].val]

    def reduce_LineString_COMMA_Point(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class PolygonTaggedText(Nonterm):
    def reduce_POLYGON_PolygonText(self, *kids):
        self.val = self.make_obj(*kids)


class PolygonText(Nonterm):
    def reduce_PolygonInParens(self, *kids):
        self.val = kids[0].val

    def reduce_EMPTY(self, *kids):
        self.val = ()


class PolygonInParens(Nonterm):
    def reduce_LPARENTHESIS_Polygon_RPARENTHESIS(self, *kids):
        self.val = kids[1].val


class Polygon(Nonterm):
    def reduce_LineStringInParens(self, *kids):
        self.val = [kids[0].val]

    def reduce_Polygon_COMMA_LineStringInParens(self, *kids):
        self.val = kids[0].val + [kids[2].val]
