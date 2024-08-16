DESC_REWRITES = {
    'st_buffer':
    '''Returns a geometry covering all points within a given distance from a geometry.''',
    'st_asbinary': """
    Returns a geometry/geography in WKB format without SRID meta data.

    Returns the OGC/ISO Well-Known Binary (WKB) representation of the
    geometry/geography without SRID meta data.
    """,
    'st_asewkb': """
    Returns a geometry in EWKB format with SRID meta data.

    Returns the Extended Well-Known Binary (EWKB) representation of the
    geometry with SRID meta data.
    """,
    'st_asewkt': """
    Returns a geometry in WKT format with SRID meta data.

    Returns the Well-Known Text (WKT) representation of the geometry with SRID
    meta data.
    """,
    'st_ashexewkb': """
    Returns a geometry in HEXEWKB format (as text).

    Returnss a geometry in HEXEWKB format (as text) using either little-endian
    (NDR) or big-endian (XDR) encoding.
    """,
    'st_astext': """
    Returns a geometry/geography in WKT format without SRID metadata.

    Returns the Well-Known Text (WKT) representation of the geometry/geography
    without SRID metadata.
    """,
    'st_asx3d': """
    Returns a geometry in X3D format.

    Returns a geometry in X3D xml node element format:
    ISO-IEC-19776-1.2-X3DEncodings-XML.
    """,
    'st_closestpoint': """
    Returns the 2D point of the first geometry closest to the second.

    Returns the 2D point of the first geometry/geography that is closest to
    the second geometry/geography. This is the first point of the shortest
    line from one geometry to the other.
    """,
    'st_3dclosestpoint': """
    Returns the 3D point of the first geometry closest to the second.

    Returns the 3D point of the first geometry/geography that is closest to
    the second geometry/geography. This is the first point of the 3D shortest
    line.
    """,
    'st_containsproperly':
    """Tests if every point of *geom2* lies in the interior of *geom1*.""",
    'st_cpawithin': """
    Tests if two trajectoriesis approach within the specified distance.

    Tests if the closest point of approach of two trajectoriesis within the
    specified distance.
    """,
    'st_dfullywithin':
    """Tests if two geometries are entirely within a given distance.""",
    'st_difference': """
    Computes a geometry resulting from removing all points in *geom2* from
    *geom1*.

    Computes a geometry representing the part of geometry *geom1* that does
    not intersect geometry *geom2*.
    """,
    'st_3ddistance': """
    Returns the 3D cartesian minimum distance between two geometries.

    Returns the 3D cartesian minimum distance (based on spatial ref) between
    two geometries in projected units.
    """,
    'st_forcerhr': """
    Forces the orientation of the vertices in a polygon to follow the RHR.

    Forces the orientation of the vertices in a polygon to follow a
    Right-Hand-Rule, in which the area that is bounded by the polygon is to
    the right of the boundary. In particular, the exterior ring is orientated
    in a clockwise direction and the interior rings in a counter-clockwise
    direction.
    """,
    'st_geogfromtext': """
    Creates a geography value from WKT or EWTK.

    Creates a geography value from Well-Known Text or Extended
    Well-Known Text representation.
    """,
    'st_geogfromwkb': """
    Creates a geography value from WKB or EWKB.

    Creates a geography value from a Well-Known Binary geometry representation
    (WKB) or extended Well Known Binary (EWKB).
    """,
    'st_geomcollfromtext': """
    Makes a collection Geometry from collection WKT.

    Makes a collection Geometry from collection WKT with the given SRID. If
    SRID is not given, it defaults to 0.
    """,
    'st_geomfromtext': """
    Creates a geometry value from WKT representation.

    Creates a geometry value from Well-Known Text representation (WKT).
    """,
    'st_geomfromewkb': """
    Creates a geometry value from EWKB.

    Creates a geometry value from Extended Well-Known Binary representation
    (EWKB).
    """,
    'st_geomfromewkt': """
    Creates a geometry value from EWKT representation.

    Creates a geometry value from Extended Well-Known Text representation (EWKT).
    """,
    'st_geomfromgeojson': """
    Creates a geometry value from a geojson representation of a geometry.

    Takes as input a geojson representation of a geometry and outputs a
    ``geometry`` value.
    """,
    'st_geomfromgml': """
    Creates a geometry value from GML representation of a geometry.

    Takes as input GML representation of geometry and outputs a  ``geometry``
    value.
    """,
    'st_geomfromkml': """
    Creates a geometry value from KML representation of a geometry.

    Takes as input KML representation of geometry and outputs a ``geometry``
    value.
    """,
    'st_geomfromwkb': """
    Creates a geometry value from WKB representation.

    Creates a geometry value from a Well-Known Binary geometry representation
    (WKB) and optional SRID.
    """,
    'st_3dintersects': """
    Tests if two geometries spatially intersect in 3D.

    Tests if two geometries spatially intersect in 3D - only for points,
    linestrings, polygons, polyhedral surface (area).
    """,
    'st_isclosed': """
    Tests if a geometry in 2D or 3D is closed.

    Tests if a LineStrings's start and end points are coincident. For a
    PolyhedralSurface tests if it is closed (volumetric).
    """,
    'st_ispolygonccw': """
    Tests counter-clockwise poligonal orientation of a geometry.

    Tests if Polygons have exterior rings oriented counter-clockwise and
    interior rings oriented clockwise.
    """,
    'st_ispolygoncw': """
    Tests clockwise poligonal orientation of a geometry.

    Tests if Polygons have exterior rings oriented clockwise and interior
    rings oriented counter-clockwise.
    """,
    'st_linefromtext': """
    Creates a geometry from WKT LINESTRING.

    Makes a Geometry from WKT representation with the given SRID. If SRID is
    not given, it defaults to 0.
    """,
    'st_3dmaxdistance': """
    Returns the 3D cartesian maximum distance between two geometries.

    Returns the 3D cartesian maximum distance (based on spatial ref) between
    two geometries in projected units.
    """,
    'st_mpointfromtext': """
    Creates a geometry from WKT MULTIPOINT.

    Makes a Geometry from WKT with the given SRID. If SRID is not given, it
    defaults to 0.
    """,
    'st_mpolyfromtext': """
    Creates a geometry from WKT MULTIPOLYGON.

    Makes a MultiPolygon Geometry from WKT with the given SRID. If SRID is not
    given, it defaults to 0.
    """,
    'st_numpatches':
    """Return the number of faces on a Polyhedral Surface.""",
    'st_orderingequals': """
    Tests if two geometries are the same geometry including points order.

    Tests if two geometries represent the same geometry and have points in the same directional order.
    """,
    'st_overlaps': """
    Tests if two geometries overlap.

    Tests if two geometries have the same dimension and intersect, but each
    has at least one point not in the other.
    """,
    'st_pointfromtext': """
    Makes a POINT geometry from WKT.

    Makes a POINT geometry from WKT with the given SRID. If SRID is not given,
    it defaults to unknown.
    """,
    'st_pointn': """
    Returns the Nth point in the first LineString in a geometry.

    Returns the Nth point in the first LineString or circular LineString in a
    geometry.
    """,
    'st_polygonfromtext': """
    Creates a geometry from WKT POLYGON.

    Makes a Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.
    """,
    'st_project':
    """Returns a point projected from a start point by a distance and bearing.""",
    'st_relate': """
    Tests if two geometries have a topological relationship.

    Tests if two geometries have a topological relationship matching an
    Intersection Matrix pattern, or computes their Intersection Matrix.
    """,
    'st_segmentize': """
    Makes a new geometry/geography with no segment longer than a given
    distance.
    """,
    'st_symdifference': """Merges two geometries excluding where they intersect.""",
    'st_touches': """
    Tests if two geometries touch without intersecting interiors.

    Tests if two geometries have at least one point in common, but their interiors do not intersect.
    """,
    'st_transform': """
    Transforms a geometry to a different spatial reference system.

    Returns a new geometry with coordinates transformed to a different spatial reference system.
    """,
    'st_coverageunion': """
    Computes polygonal coverage from a set of polygons.

    Computes the union of a set of polygons forming a coverage by removing
    shared edges.

    """,
    'st_polygonize': """
    Computes a collection of polygons formed from a set of linework.

    Computes a collection of polygons formed from the linework of a set of
    geometries.
    """,
}

FUNC_CATEGORIES = {
    'Geometry Constructors': [
        'ST_Collect',
        'ST_LineFromMultiPoint',
        'ST_MakeEnvelope',
        'ST_MakeLine',
        'ST_MakePoint',
        'ST_MakePointM',
        'ST_MakePolygon',
        'ST_Point',
        'ST_PointZ',
        'ST_PointM',
        'ST_PointZM',
        'ST_Polygon',
        'ST_TileEnvelope',
        'ST_HexagonGrid',
        'ST_Hexagon',
        'ST_SquareGrid',
        'ST_Square',
        'ST_Letters',
    ],
    'Geometry Accessors': [
        'GeometryType',
        'ST_Boundary',
        'ST_BoundingDiagonal',
        'ST_CoordDim',
        'ST_Dimension',
        'ST_Dump',
        'ST_DumpPoints',
        'ST_DumpSegments',
        'ST_DumpRings',
        'ST_EndPoint',
        'ST_Envelope',
        'ST_ExteriorRing',
        'ST_GeometryN',
        'ST_GeometryType',
        'ST_HasArc',
        'ST_InteriorRingN',
        'ST_IsClosed',
        'ST_IsCollection',
        'ST_IsEmpty',
        'ST_IsPolygonCCW',
        'ST_IsPolygonCW',
        'ST_IsRing',
        'ST_IsSimple',
        'ST_M',
        'ST_MemSize',
        'ST_NDims',
        'ST_NPoints',
        'ST_NRings',
        'ST_NumGeometries',
        'ST_NumInteriorRings',
        'ST_NumInteriorRing',
        'ST_NumPatches',
        'ST_NumPoints',
        'ST_PatchN',
        'ST_PointN',
        'ST_Points',
        'ST_StartPoint',
        'ST_Summary',
        'ST_X',
        'ST_Y',
        'ST_Z',
        'ST_Zmflag',
    ],
    'Geometry Editors': [
        'ST_AddPoint',
        'ST_CollectionExtract',
        'ST_CollectionHomogenize',
        'ST_CurveToLine',
        'ST_Scroll',
        'ST_FlipCoordinates',
        'ST_Force2D',
        'ST_Force3D',
        'ST_Force3DZ',
        'ST_Force3DM',
        'ST_Force4D',
        'ST_ForcePolygonCCW',
        'ST_ForceCollection',
        'ST_ForcePolygonCW',
        'ST_ForceSFS',
        'ST_ForceRHR',
        'ST_ForceCurve',
        'ST_LineToCurve',
        'ST_Multi',
        'ST_LineExtend',
        'ST_Normalize',
        'ST_Project',
        'ST_QuantizeCoordinates',
        'ST_RemovePoint',
        'ST_RemoveRepeatedPoints',
        'ST_Reverse',
        'ST_Segmentize',
        'ST_SetPoint',
        'ST_ShiftLongitude',
        'ST_WrapX',
        'ST_SnapToGrid',
        'ST_Snap',
        'ST_SwapOrdinates',
    ],
    'Geometry Validation': [
        'ST_IsValid',
        'ST_IsValidDetail',
        'ST_IsValidReason',
        'ST_MakeValid',
    ],
    'Spatial Reference System Functions': [
        'ST_InverseTransformPipeline',
        'ST_SetSRID',
        'ST_SRID',
        'ST_Transform',
        'ST_TransformPipeline',
        'postgis_srs_codes',
        'postgis_srs',
        'postgis_srs_all',
        'postgis_srs_search',
    ],
    'Well-Known Text (WKT)': [
        'ST_BdPolyFromText',
        'ST_BdMPolyFromText',
        'ST_GeogFromText',
        'ST_GeographyFromText',
        'ST_GeomCollFromText',
        'ST_GeomFromEWKT',
        'ST_GeomFromMARC21',
        'ST_GeometryFromText',
        'ST_GeomFromText',
        'ST_LineFromText',
        'ST_MLineFromText',
        'ST_MPointFromText',
        'ST_MPolyFromText',
        'ST_PointFromText',
        'ST_PolygonFromText',
        'ST_WKTToSQL',
        'ST_AsEWKT',
        'ST_AsText',
    ],
    'Well-Known Binary (WKB)': [
        'ST_GeogFromWKB',
        'ST_GeomFromEWKB',
        'ST_GeomFromWKB',
        'ST_LineFromWKB',
        'ST_LinestringFromWKB',
        'ST_PointFromWKB',
        'ST_WKBToSQL',
        'ST_AsBinary',
        'ST_AsEWKB',
        'ST_AsHEXEWKB',
    ],
    'Other Formats': [
        'ST_Box2dFromGeoHash',
        'ST_GeomFromGeoHash',
        'ST_GeomFromGML',
        'ST_GeomFromGeoJSON',
        'ST_GeomFromKML',
        'ST_GeomFromTWKB',
        'ST_GMLToSQL',
        'ST_LineFromEncodedPolyline',
        'ST_PointFromGeoHash',
        'ST_FromFlatGeobufToTable',
        'ST_FromFlatGeobuf',
        'ST_AsEncodedPolyline',
        'ST_AsFlatGeobuf',
        'ST_AsGeobuf',
        'ST_AsGeoJSON',
        'ST_AsGML',
        'ST_AsKML',
        'ST_AsLatLonText',
        'ST_AsMARC21',
        'ST_AsMVTGeom',
        'ST_AsMVT',
        'ST_AsSVG',
        'ST_AsTWKB',
        'ST_AsX3D',
        'ST_GeoHash',
    ],
    'Topological Relationships': [
        'ST_3DIntersects',
        'ST_Contains',
        'ST_ContainsProperly',
        'ST_CoveredBy',
        'ST_Covers',
        'ST_Crosses',
        'ST_Disjoint',
        'ST_Equals',
        'ST_Intersects',
        'ST_LineCrossingDirection',
        'ST_OrderingEquals',
        'ST_Overlaps',
        'ST_Relate',
        'ST_RelateMatch',
        'ST_Touches',
        'ST_Within',
    ],
    'Distance Relationships': [
        'ST_3DDWithin',
        'ST_3DDFullyWithin',
        'ST_DFullyWithin',
        'ST_DWithin',
        'ST_PointInsideCircle',
    ],
    'Measurement Functions': [
        'ST_Area',
        'ST_Azimuth',
        'ST_Angle',
        'ST_ClosestPoint',
        'ST_3DClosestPoint',
        'ST_Distance',
        'ST_3DDistance',
        'ST_DistanceSphere',
        'ST_DistanceSpheroid',
        'ST_FrechetDistance',
        'ST_HausdorffDistance',
        'ST_Length',
        'ST_Length2D',
        'ST_3DLength',
        'ST_LengthSpheroid',
        'ST_LongestLine',
        'ST_3DLongestLine',
        'ST_MaxDistance',
        'ST_3DMaxDistance',
        'ST_MinimumClearance',
        'ST_MinimumClearanceLine',
        'ST_Perimeter',
        'ST_Perimeter2D',
        'ST_3DPerimeter',
        'ST_ShortestLine',
        'ST_3DShortestLine',
    ],
    'Overlay Functions': [
        'ST_ClipByBox2D',
        'ST_Difference',
        'ST_Intersection',
        'ST_MemUnion',
        'ST_Node',
        'ST_Split',
        'ST_Subdivide',
        'ST_SymDifference',
        'ST_UnaryUnion',
        'ST_Union',
    ],
    'Geometry Processing': [
        'ST_Buffer',
        'ST_BuildArea',
        'ST_Centroid',
        'ST_ChaikinSmoothing',
        'ST_ConcaveHull',
        'ST_ConvexHull',
        'ST_DelaunayTriangles',
        'ST_FilterByM',
        'ST_GeneratePoints',
        'ST_GeometricMedian',
        'ST_LineMerge',
        'ST_MaximumInscribedCircle',
        'ST_LargestEmptyCircle',
        'ST_MinimumBoundingCircle',
        'ST_MinimumBoundingRadius',
        'ST_OrientedEnvelope',
        'ST_OffsetCurve',
        'ST_PointOnSurface',
        'ST_Polygonize',
        'ST_ReducePrecision',
        'ST_SharedPaths',
        'ST_Simplify',
        'ST_SimplifyPreserveTopology',
        'ST_SimplifyPolygonHull',
        'ST_SimplifyVW',
        'ST_SetEffectiveArea',
        'ST_TriangulatePolygon',
        'ST_VoronoiLines',
        'ST_VoronoiPolygons',
    ],
    'Coverages': [
        'ST_CoverageInvalidEdges',
        'ST_CoverageSimplify',
        'ST_CoverageUnion',
    ],
    'Affine Transformations': [
        'ST_Affine',
        'ST_Rotate',
        'ST_RotateX',
        'ST_RotateY',
        'ST_RotateZ',
        'ST_Scale',
        'ST_Translate',
        'ST_TransScale',
    ],
    'Clustering Functions': [
        'ST_ClusterDBSCAN',
        'ST_ClusterIntersecting',
        'ST_ClusterIntersectingWin',
        'ST_ClusterKMeans',
        'ST_ClusterWithin',
        'ST_ClusterWithinWin',
    ],
    'Bounding Box Functions': [
        'Box2D',
        'Box3D',
        'ST_EstimatedExtent',
        'ST_Expand',
        'ST_Extent',
        'ST_3DExtent',
        'ST_MakeBox2D',
        'ST_3DMakeBox',
        'ST_XMax',
        'ST_XMin',
        'ST_YMax',
        'ST_YMin',
        'ST_ZMax',
        'ST_ZMin',
    ],
    'Linear Referencing': [
        'ST_LineInterpolatePoint',
        'ST_3DLineInterpolatePoint',
        'ST_LineInterpolatePoints',
        'ST_LineLocatePoint',
        'ST_LineSubstring',
        'ST_LocateAlong',
        'ST_LocateBetween',
        'ST_LocateBetweenElevations',
        'ST_InterpolatePoint',
        'ST_AddMeasure',
    ],
    'Trajectory Functions': [
        'ST_IsValidTrajectory',
        'ST_ClosestPointOfApproach',
        'ST_DistanceCPA',
        'ST_CPAWithin',
    ],
}