.. _ref_reference_postgis:

.. versionadded:: 6.0

============
ext::postgis
============

This extension exposes the functionality of the `PostGIS <postgis_>`_ library. 
The scope of the EdgeDB extension is to adapt the types and functions with 
minimal changes.

As a rule, many of the functions in PostGIS library have a ``ST_``` prefix, 
however, we omitted it since in EdgeDB all these functions would already be in 
the ``ext::postgis`` namespace.

Types
=====

There are four basic scalar types introduced by this extension:

.. eql:type:: postgis::geometry

   The type representing 2- or 3-dimensional spatial features.

   By default most of the ``geometry`` values are assumed to be representing 
   planar geometry in a Cartesian coordinate system.

   Every other ``ext::postgis`` scalar type is castable into ``geometry``. Many 
   of the PostGIS functions only accept ``geometry`` as input.

----------

.. eql:type:: postgis::geography

   The type representing spatial features with geodetic coordinate systems.

   The PostGIS ``geography`` data type provides native support for spatial 
   features represented on "geographic" coordinates (sometimes called "geodetic" 
   coordinates, or "lat/lon", or "lon/lat"). Geographic coordinates are spherical 
   coordinates expressed in angular units (degrees).

----------

.. eql:type:: postgis::box2d

   The type representing a 2-dimensional bounding box.

----------

.. eql:type:: postgis::box3d

   The type representing a 3-dimensional bounding box.


Operators
=========

The extension provides spatial operators optimized for spatial indexing. These 
operators are exposed as functions with the ``op_`` prefix and work with spatial 
indexes (``pg::gist``, ``pg::brin``, and ``pg::spgist``).

.. list-table::
   :class: funcoptable

   * - :eql:func:`ext::postgis::op_above`
     - Above operator (\|>>)
   * - :eql:func:`ext::postgis::op_below`
     - Below operator (<<\|)
   * - :eql:func:`ext::postgis::op_contained_3d`
     - 3D contained operator (<<@)
   * - :eql:func:`ext::postgis::op_contains`
     - Contains operator (~)
   * - :eql:func:`ext::postgis::op_contains_2d`
     - 2D contains operator (~)
   * - :eql:func:`ext::postgis::op_contains_3d`
     - 3D contains operator (@>>)
   * - :eql:func:`ext::postgis::op_contains_nd`
     - N-D contains operator (~~)
   * - :eql:func:`ext::postgis::op_distance_box`
     - Box distance operator (<#>)
   * - :eql:func:`ext::postgis::op_distance_centroid`
     - Centroid distance operator (<->)
   * - :eql:func:`ext::postgis::op_distance_centroid_nd`
     - N-D centroid distance operator (<<->>)
   * - :eql:func:`ext::postgis::op_distance_cpa`
     - Closest point of approach operator (\|=\|)
   * - :eql:func:`ext::postgis::op_distance_knn`
     - K-nearest neighbor operator (<->)
   * - :eql:func:`ext::postgis::op_is_contained_2d`
     - 2D contained operator (@)
   * - :eql:func:`ext::postgis::op_left`
     - Left operator (<<)
   * - :eql:func:`ext::postgis::op_neq`
     - Not equal operator (<>)
   * - :eql:func:`ext::postgis::op_overabove`
     - Over-above operator (\|&>)
   * - :eql:func:`ext::postgis::op_overbelow`
     - Over-below operator (&<\|)
   * - :eql:func:`ext::postgis::op_overlaps`
     - Overlaps operator (&&)
   * - :eql:func:`ext::postgis::op_overlaps_2d`
     - 2D overlaps operator (&&)
   * - :eql:func:`ext::postgis::op_overlaps_3d`
     - 3D overlaps operator (&/&)
   * - :eql:func:`ext::postgis::op_overlaps_nd`
     - N-D overlaps operator (&&&)
   * - :eql:func:`ext::postgis::op_overleft`
     - Over-left operator (&<)
   * - :eql:func:`ext::postgis::op_overright`
     - Over-right operator (&>)
   * - :eql:func:`ext::postgis::op_right`
     - Right operator (>>)
   * - :eql:func:`ext::postgis::op_same`
     - Exactly equal operator (~=)
   * - :eql:func:`ext::postgis::op_same_3d`
     - 3D exactly equal operator (~==)
   * - :eql:func:`ext::postgis::op_same_nd`
     - N-D exactly equal operator (~~=)
   * - :eql:func:`ext::postgis::op_within`
     - Within operator (@)
   * - :eql:func:`ext::postgis::op_within_nd`
     - N-D within operator (@@)


------------


.. eql:function:: ext::postgis::op_above( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``|>>`` operator.


------------


.. eql:function:: ext::postgis::op_below( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``<<|`` operator.


------------


.. eql:function:: ext::postgis::op_contained_3d( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``<<@`` operator.


------------


.. eql:function:: ext::postgis::op_contains( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``~`` operator.


------------


.. eql:function:: ext::postgis::op_contains_2d( \
                   a: ext::postgis::box2d, \
                   b: ext::postgis::box2d, \
                 ) ->  std::bool
                 ext::postgis::op_contains_2d( \
                   a: ext::postgis::box2d, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool
                 ext::postgis::op_contains_2d( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::box2d, \
                 ) ->  std::bool

   This is exposing the ``~`` operator.


------------


.. eql:function:: ext::postgis::op_contains_3d( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``@>>`` operator.


------------


.. eql:function:: ext::postgis::op_contains_nd( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``~~`` operator.


------------


.. eql:function:: ext::postgis::op_distance_box( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::float64

   This is exposing the ``<#>`` operator.


------------


.. eql:function:: ext::postgis::op_distance_centroid( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::float64

   This is exposing the ``<->`` operator.


------------


.. eql:function:: ext::postgis::op_distance_centroid_nd( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::float64

   This is exposing the ``<<->>`` operator.


------------


.. eql:function:: ext::postgis::op_distance_cpa( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::float64

   This is exposing the ``|=|`` operator.


------------


.. eql:function:: ext::postgis::op_distance_knn( \
                   a: ext::postgis::geography, \
                   b: ext::postgis::geography, \
                 ) ->  std::float64

   This is exposing the ``<->`` operator.


------------


.. eql:function:: ext::postgis::op_is_contained_2d( \
                   a: ext::postgis::box2d, \
                   b: ext::postgis::box2d, \
                 ) ->  std::bool
                 ext::postgis::op_is_contained_2d( \
                   a: ext::postgis::box2d, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool
                 ext::postgis::op_is_contained_2d( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::box2d, \
                 ) ->  std::bool

   This is exposing the ``@`` operator.


------------


.. eql:function:: ext::postgis::op_left( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``<<`` operator.


------------


.. eql:function:: ext::postgis::op_neq( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``<>`` operator.


------------


.. eql:function:: ext::postgis::op_overabove( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``|&>`` operator.


------------


.. eql:function:: ext::postgis::op_overbelow( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``&<|`` operator.


------------


.. eql:function:: ext::postgis::op_overlaps( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool
                 ext::postgis::op_overlaps( \
                   a: ext::postgis::geography, \
                   b: ext::postgis::geography, \
                 ) ->  std::bool

   This is exposing the ``&&`` operator.


------------


.. eql:function:: ext::postgis::op_overlaps_2d( \
                   a: ext::postgis::box2d, \
                   b: ext::postgis::box2d, \
                 ) ->  std::bool
                 ext::postgis::op_overlaps_2d( \
                   a: ext::postgis::box2d, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool
                 ext::postgis::op_overlaps_2d( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::box2d, \
                 ) ->  std::bool

   This is exposing the ``&&`` operator.


------------


.. eql:function:: ext::postgis::op_overlaps_3d( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``&/&`` operator.


------------


.. eql:function:: ext::postgis::op_overlaps_nd( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``&&&`` operator.


------------


.. eql:function:: ext::postgis::op_overleft( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``&<`` operator.


------------


.. eql:function:: ext::postgis::op_overright( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``&>`` operator.


------------


.. eql:function:: ext::postgis::op_right( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``>>`` operator.


------------


.. eql:function:: ext::postgis::op_same( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``~=`` operator.


------------


.. eql:function:: ext::postgis::op_same_3d( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``~==`` operator.


------------


.. eql:function:: ext::postgis::op_same_nd( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``~~=`` operator.


------------


.. eql:function:: ext::postgis::op_within( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``@`` operator.


------------


.. eql:function:: ext::postgis::op_within_nd( \
                   a: ext::postgis::geometry, \
                   b: ext::postgis::geometry, \
                 ) ->  std::bool

   This is exposing the ``@@`` operator.

Common Functions
================

Here are the most commonly used functions. For a complete list of all available 
functions, refer to the `PostGIS documentation <postgis_>`_ - the EdgeDB functions 
match PostGIS functions but without the ``ST_`` prefix.

.. list-table::
   :class: funcoptable

   * - :eql:func:`ext::postgis::makepoint`
     - Create point geometry
   * - :eql:func:`ext::postgis::makeline`
     - Create line geometry
   * - :eql:func:`ext::postgis::makepolygon`  
     - Create polygon geometry
   * - :eql:func:`ext::postgis::buffer`
     - Create buffer around geometry
   * - :eql:func:`ext::postgis::distance`
     - Calculate distance between geometries
   * - :eql:func:`ext::postgis::area`
     - Calculate area
   * - :eql:func:`ext::postgis::length`
     - Calculate length
   * - :eql:func:`ext::postgis::perimeter`
     - Calculate perimeter
   * - :eql:func:`ext::postgis::contains`
     - Test if one geometry contains another
   * - :eql:func:`ext::postgis::intersects`
     - Test if geometries intersect
   * - :eql:func:`ext::postgis::within`
     - Test if one geometry is within another
   * - :eql:func:`ext::postgis::transform`
     - Transform geometry to different coordinate system
   * - :eql:func:`ext::postgis::astext`
     - Convert geometry to WKT format
   * - :eql:func:`ext::postgis::geomfromtext`
     - Create geometry from WKT format

The following sections document the most commonly used functions:

.. eql:function:: ext::postgis::makepoint( \
                   x: std::float64, \
                   y: std::float64 \
                 ) -> ext::postgis::geometry
                 ext::postgis::makepoint( \
                   x: std::float64, \
                   y: std::float64, \
                   z: std::float64 \
                 ) -> ext::postgis::geometry
                 ext::postgis::makepoint( \
                   x: std::float64, \
                   y: std::float64, \
                   z: std::float64, \
                   m: std::float64 \
                 ) -> ext::postgis::geometry

   Create a point geometry with the given coordinates.


------------


.. eql:function:: ext::postgis::makeline( \
                   points: array<ext::postgis::geometry> \
                 ) -> ext::postgis::geometry
                 ext::postgis::makeline( \
                   start_point: ext::postgis::geometry, \
                   end_point: ext::postgis::geometry \
                 ) -> ext::postgis::geometry

   Create a linestring geometry from points.


------------


.. eql:function:: ext::postgis::makepolygon( \
                   shell: ext::postgis::geometry \
                 ) -> ext::postgis::geometry
                 ext::postgis::makepolygon( \
                   shell: ext::postgis::geometry, \
                   holes: array<ext::postgis::geometry> \
                 ) -> ext::postgis::geometry

   Create a polygon from an outer ring (shell) and optional array of holes.


------------


.. eql:function:: ext::postgis::buffer( \
                   geometry: ext::postgis::geometry, \
                   radius: std::float64 \
                 ) -> ext::postgis::geometry
                 ext::postgis::buffer( \
                   geography: ext::postgis::geography, \
                   radius: std::float64 \
                 ) -> ext::postgis::geography

   Create a geometry covering all points within given distance from input.


------------


.. eql:function:: ext::postgis::distance( \
                   geom1: ext::postgis::geometry, \
                   geom2: ext::postgis::geometry \
                 ) -> std::float64
                 ext::postgis::distance( \
                   geog1: ext::postgis::geography, \
                   geog2: ext::postgis::geography, \
                   use_spheroid: std::bool = true \
                 ) -> std::float64

   Calculate the shortest distance between two geometries.


------------


.. eql:function:: ext::postgis::area( \
                   geometry: ext::postgis::geometry \
                 ) -> std::float64
                 ext::postgis::area( \
                   geography: ext::postgis::geography, \
                   use_spheroid: std::bool = true \
                 ) -> std::float64

   Calculate the area of a geometry.


------------


.. eql:function:: ext::postgis::length( \
                   geometry: ext::postgis::geometry \
                 ) -> std::float64
                 ext::postgis::length( \
                   geography: ext::postgis::geography, \
                   use_spheroid: std::bool = true \
                 ) -> std::float64

   Calculate the length of a linestring or perimeter of a polygon.


------------


.. eql:function:: ext::postgis::perimeter( \
                   geometry: ext::postgis::geometry \
                 ) -> std::float64
                 ext::postgis::perimeter( \
                   geography: ext::postgis::geography, \
                   use_spheroid: std::bool = true \
                 ) -> std::float64

   Calculate the perimeter of a geometry.


------------


.. eql:function:: ext::postgis::contains( \
                   geom1: ext::postgis::geometry, \
                   geom2: ext::postgis::geometry \
                 ) -> std::bool

   Test if the first geometry contains the second geometry.


------------


.. eql:function:: ext::postgis::intersects( \
                   geom1: ext::postgis::geometry, \
                   geom2: ext::postgis::geometry \
                 ) -> std::bool
                 ext::postgis::intersects( \
                   geog1: ext::postgis::geography, \
                   geog2: ext::postgis::geography \
                 ) -> std::bool

   Test if two geometries intersect.


------------


.. eql:function:: ext::postgis::within( \
                   geom1: ext::postgis::geometry, \
                   geom2: ext::postgis::geometry \
                 ) -> std::bool

   Test if the first geometry is completely within the second geometry.


------------


.. eql:function:: ext::postgis::transform( \
                   geometry: ext::postgis::geometry, \
                   srid: std::int64 \
                 ) -> ext::postgis::geometry

   Transform a geometry into a different spatial reference system.


------------


.. eql:function:: ext::postgis::astext( \
                   geometry: ext::postgis::geometry \
                 ) -> std::str
                 ext::postgis::astext( \
                   geography: ext::postgis::geography \
                 ) -> std::str

   Return the Well-Known Text (WKT) representation of the geometry.


------------


.. eql:function:: ext::postgis::geomfromtext( \
                   wkt: std::str \
                 ) -> ext::postgis::geometry
                 ext::postgis::geomfromtext( \
                   wkt: std::str, \
                   srid: std::int64 \
                 ) -> ext::postgis::geometry

   Create a geometry from its WKT representation.


For more information about PostGIS extension, refer to 
the `PostGIS documentation <postgis_>`_.

.. _postgis: https://postgis.net/docs/manual-3.5/
