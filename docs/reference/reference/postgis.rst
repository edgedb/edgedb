.. versionadded:: 6.0

.. _ref_ext_postgis:

============
ext::postgis
============

This extension exposes the functionality of the `PostGIS <postgis_>`_ library. It is a vast library dedicated to handling geographic and various geometric data. The scope of the EdgeDB extension is to mainly adapt the types and functions used in this library with minimal changes.

As a rule, many of the functions in PostGIS library have a ``ST_``` prefix, however, we omitted it since in EdgeDB all these functions would already be in the ``ext::postgis`` namespace and additional disambiguation is unnecessary.


Types
=====

There are four basic scalar types introduced by this extension:

----------


.. eql:type:: postgis::geometry

    The type representing 2- or 3-dimensional spatial features.

    By default most of the ``geometry`` values are assumed to be representing planar geometry in a Cartesian coordinate system.

    Every other ``ext::postgis`` scalar type is castable into ``geometry``. Many of the PostGIS functions only accept ``geometry`` as input.


----------


.. eql:type:: postgis::geography

    The type representing spatial features with geodetic coordinate systems.

    The PostGIS ``geography`` data type provides native support for spatial features represented on "geographic" coordinates (sometimes called "geodetic" coordinates, or "lat/lon", or "lon/lat"). Geographic coordinates are spherical coordinates expressed in angular units (degrees).


----------


.. eql:type:: postgis::box2d

    The type representing a 2-dimensional bounding box.


----------


.. eql:type:: postgis::box3d

    The type representing a 3-dimensional bounding box.


Operators
=========

There are many functions available for processing all this geometric and geographic data. Of note are the functions that represent *operations* affected by the indexes (``pg::gist``, ``pg::brin``, and ``pg::spgist``). These functions all have a ``op_`` prefix to help identify them.

----------


.. eql:function:: ext::postgis::op_above( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``|>>`` operator.


----------


.. eql:function:: ext::postgis::op_below( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``<<|`` operator.


----------


.. eql:function:: ext::postgis::op_contained_3d( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``<<@`` operator.


----------


.. eql:function:: ext::postgis::op_contains( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``~`` operator.


----------


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


----------


.. eql:function:: ext::postgis::op_contains_3d( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``@>>`` operator.


----------


.. eql:function:: ext::postgis::op_contains_nd( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``~~`` operator.


----------


.. eql:function:: ext::postgis::op_distance_box( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing the ``<#>`` operator.


----------


.. eql:function:: ext::postgis::op_distance_centroid( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing the ``<->`` operator.


----------


.. eql:function:: ext::postgis::op_distance_centroid_nd( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing the ``<<->>`` operator.


----------


.. eql:function:: ext::postgis::op_distance_cpa( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing the ``|=|`` operator.


----------


.. eql:function:: ext::postgis::op_distance_knn( \
                    a: ext::postgis::geography, \
                    b: ext::postgis::geography, \
                  ) ->  std::float64

    This is exposing the ``<->`` operator.


----------


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


----------


.. eql:function:: ext::postgis::op_left( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``<<`` operator.


----------


.. eql:function:: ext::postgis::op_neq( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``<>`` operator.


----------


.. eql:function:: ext::postgis::op_overabove( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``|&>`` operator.


----------


.. eql:function:: ext::postgis::op_overbelow( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``&<|`` operator.


----------


.. eql:function:: ext::postgis::op_overlaps( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool
                  ext::postgis::op_overlaps( \
                    a: ext::postgis::geography, \
                    b: ext::postgis::geography, \
                  ) ->  std::bool

    This is exposing the ``&&`` operator.


----------


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


----------


.. eql:function:: ext::postgis::op_overlaps_3d( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``&/&`` operator.


----------


.. eql:function:: ext::postgis::op_overlaps_nd( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``&&&`` operator.


----------


.. eql:function:: ext::postgis::op_overleft( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``&<`` operator.


----------


.. eql:function:: ext::postgis::op_overright( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``&>`` operator.


----------


.. eql:function:: ext::postgis::op_right( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``>>`` operator.


----------


.. eql:function:: ext::postgis::op_same( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``~=`` operator.


----------


.. eql:function:: ext::postgis::op_same_3d( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``~==`` operator.


----------


.. eql:function:: ext::postgis::op_same_nd( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``~~=`` operator.


----------


.. eql:function:: ext::postgis::op_within( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``@`` operator.


----------


.. eql:function:: ext::postgis::op_within_nd( \
                    a: ext::postgis::geometry, \
                    b: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing the ``@@`` operator.


Functions
=========

The core functions can be roughly grouped into the following categories.

Geometry Constructors
---------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::collect`
      - :eql:func-desc:`ext::postgis::collect`

    * - :eql:func:`ext::postgis::hexagon`
      - :eql:func-desc:`ext::postgis::hexagon`

    * - :eql:func:`ext::postgis::linefrommultipoint`
      - :eql:func-desc:`ext::postgis::linefrommultipoint`

    * - :eql:func:`ext::postgis::makeenvelope`
      - :eql:func-desc:`ext::postgis::makeenvelope`

    * - :eql:func:`ext::postgis::makeline`
      - :eql:func-desc:`ext::postgis::makeline`

    * - :eql:func:`ext::postgis::makepoint`
      - :eql:func-desc:`ext::postgis::makepoint`

    * - :eql:func:`ext::postgis::makepointm`
      - :eql:func-desc:`ext::postgis::makepointm`

    * - :eql:func:`ext::postgis::makepolygon`
      - :eql:func-desc:`ext::postgis::makepolygon`

    * - :eql:func:`ext::postgis::point`
      - :eql:func-desc:`ext::postgis::point`

    * - :eql:func:`ext::postgis::pointm`
      - :eql:func-desc:`ext::postgis::pointm`

    * - :eql:func:`ext::postgis::pointz`
      - :eql:func-desc:`ext::postgis::pointz`

    * - :eql:func:`ext::postgis::pointzm`
      - :eql:func-desc:`ext::postgis::pointzm`

    * - :eql:func:`ext::postgis::polygon`
      - :eql:func-desc:`ext::postgis::polygon`

    * - :eql:func:`ext::postgis::square`
      - :eql:func-desc:`ext::postgis::square`

    * - :eql:func:`ext::postgis::tileenvelope`
      - :eql:func-desc:`ext::postgis::tileenvelope`

Geometry Accessors
------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::boundary`
      - :eql:func-desc:`ext::postgis::boundary`

    * - :eql:func:`ext::postgis::boundingdiagonal`
      - :eql:func-desc:`ext::postgis::boundingdiagonal`

    * - :eql:func:`ext::postgis::coorddim`
      - :eql:func-desc:`ext::postgis::coorddim`

    * - :eql:func:`ext::postgis::dimension`
      - :eql:func-desc:`ext::postgis::dimension`

    * - :eql:func:`ext::postgis::endpoint`
      - :eql:func-desc:`ext::postgis::endpoint`

    * - :eql:func:`ext::postgis::envelope`
      - :eql:func-desc:`ext::postgis::envelope`

    * - :eql:func:`ext::postgis::exteriorring`
      - :eql:func-desc:`ext::postgis::exteriorring`

    * - :eql:func:`ext::postgis::geometryn`
      - :eql:func-desc:`ext::postgis::geometryn`

    * - :eql:func:`ext::postgis::geometrytype`
      - :eql:func-desc:`ext::postgis::geometrytype`

    * - :eql:func:`ext::postgis::hasarc`
      - :eql:func-desc:`ext::postgis::hasarc`

    * - :eql:func:`ext::postgis::interiorringn`
      - :eql:func-desc:`ext::postgis::interiorringn`

    * - :eql:func:`ext::postgis::isclosed`
      - :eql:func-desc:`ext::postgis::isclosed`

    * - :eql:func:`ext::postgis::iscollection`
      - :eql:func-desc:`ext::postgis::iscollection`

    * - :eql:func:`ext::postgis::isempty`
      - :eql:func-desc:`ext::postgis::isempty`

    * - :eql:func:`ext::postgis::ispolygonccw`
      - :eql:func-desc:`ext::postgis::ispolygonccw`

    * - :eql:func:`ext::postgis::ispolygoncw`
      - :eql:func-desc:`ext::postgis::ispolygoncw`

    * - :eql:func:`ext::postgis::isring`
      - :eql:func-desc:`ext::postgis::isring`

    * - :eql:func:`ext::postgis::issimple`
      - :eql:func-desc:`ext::postgis::issimple`

    * - :eql:func:`ext::postgis::m`
      - :eql:func-desc:`ext::postgis::m`

    * - :eql:func:`ext::postgis::memsize`
      - :eql:func-desc:`ext::postgis::memsize`

    * - :eql:func:`ext::postgis::ndims`
      - :eql:func-desc:`ext::postgis::ndims`

    * - :eql:func:`ext::postgis::npoints`
      - :eql:func-desc:`ext::postgis::npoints`

    * - :eql:func:`ext::postgis::nrings`
      - :eql:func-desc:`ext::postgis::nrings`

    * - :eql:func:`ext::postgis::numgeometries`
      - :eql:func-desc:`ext::postgis::numgeometries`

    * - :eql:func:`ext::postgis::numinteriorring`
      - :eql:func-desc:`ext::postgis::numinteriorring`

    * - :eql:func:`ext::postgis::numinteriorrings`
      - :eql:func-desc:`ext::postgis::numinteriorrings`

    * - :eql:func:`ext::postgis::numpatches`
      - :eql:func-desc:`ext::postgis::numpatches`

    * - :eql:func:`ext::postgis::numpoints`
      - :eql:func-desc:`ext::postgis::numpoints`

    * - :eql:func:`ext::postgis::patchn`
      - :eql:func-desc:`ext::postgis::patchn`

    * - :eql:func:`ext::postgis::pointn`
      - :eql:func-desc:`ext::postgis::pointn`

    * - :eql:func:`ext::postgis::points`
      - :eql:func-desc:`ext::postgis::points`

    * - :eql:func:`ext::postgis::startpoint`
      - :eql:func-desc:`ext::postgis::startpoint`

    * - :eql:func:`ext::postgis::summary`
      - :eql:func-desc:`ext::postgis::summary`

    * - :eql:func:`ext::postgis::x`
      - :eql:func-desc:`ext::postgis::x`

    * - :eql:func:`ext::postgis::y`
      - :eql:func-desc:`ext::postgis::y`

    * - :eql:func:`ext::postgis::z`
      - :eql:func-desc:`ext::postgis::z`

    * - :eql:func:`ext::postgis::zmflag`
      - :eql:func-desc:`ext::postgis::zmflag`

Geometry Editors
----------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::addpoint`
      - :eql:func-desc:`ext::postgis::addpoint`

    * - :eql:func:`ext::postgis::collectionextract`
      - :eql:func-desc:`ext::postgis::collectionextract`

    * - :eql:func:`ext::postgis::collectionhomogenize`
      - :eql:func-desc:`ext::postgis::collectionhomogenize`

    * - :eql:func:`ext::postgis::curvetoline`
      - :eql:func-desc:`ext::postgis::curvetoline`

    * - :eql:func:`ext::postgis::flipcoordinates`
      - :eql:func-desc:`ext::postgis::flipcoordinates`

    * - :eql:func:`ext::postgis::force2d`
      - :eql:func-desc:`ext::postgis::force2d`

    * - :eql:func:`ext::postgis::force3d`
      - :eql:func-desc:`ext::postgis::force3d`

    * - :eql:func:`ext::postgis::force3dm`
      - :eql:func-desc:`ext::postgis::force3dm`

    * - :eql:func:`ext::postgis::force3dz`
      - :eql:func-desc:`ext::postgis::force3dz`

    * - :eql:func:`ext::postgis::force4d`
      - :eql:func-desc:`ext::postgis::force4d`

    * - :eql:func:`ext::postgis::forcecollection`
      - :eql:func-desc:`ext::postgis::forcecollection`

    * - :eql:func:`ext::postgis::forcecurve`
      - :eql:func-desc:`ext::postgis::forcecurve`

    * - :eql:func:`ext::postgis::forcepolygonccw`
      - :eql:func-desc:`ext::postgis::forcepolygonccw`

    * - :eql:func:`ext::postgis::forcepolygoncw`
      - :eql:func-desc:`ext::postgis::forcepolygoncw`

    * - :eql:func:`ext::postgis::forcerhr`
      - :eql:func-desc:`ext::postgis::forcerhr`

    * - :eql:func:`ext::postgis::forcesfs`
      - :eql:func-desc:`ext::postgis::forcesfs`

    * - :eql:func:`ext::postgis::lineextend`
      - :eql:func-desc:`ext::postgis::lineextend`

    * - :eql:func:`ext::postgis::linetocurve`
      - :eql:func-desc:`ext::postgis::linetocurve`

    * - :eql:func:`ext::postgis::multi`
      - :eql:func-desc:`ext::postgis::multi`

    * - :eql:func:`ext::postgis::normalize`
      - :eql:func-desc:`ext::postgis::normalize`

    * - :eql:func:`ext::postgis::project`
      - :eql:func-desc:`ext::postgis::project`

    * - :eql:func:`ext::postgis::quantizecoordinates`
      - :eql:func-desc:`ext::postgis::quantizecoordinates`

    * - :eql:func:`ext::postgis::removepoint`
      - :eql:func-desc:`ext::postgis::removepoint`

    * - :eql:func:`ext::postgis::removerepeatedpoints`
      - :eql:func-desc:`ext::postgis::removerepeatedpoints`

    * - :eql:func:`ext::postgis::reverse`
      - :eql:func-desc:`ext::postgis::reverse`

    * - :eql:func:`ext::postgis::scroll`
      - :eql:func-desc:`ext::postgis::scroll`

    * - :eql:func:`ext::postgis::segmentize`
      - :eql:func-desc:`ext::postgis::segmentize`

    * - :eql:func:`ext::postgis::setpoint`
      - :eql:func-desc:`ext::postgis::setpoint`

    * - :eql:func:`ext::postgis::shiftlongitude`
      - :eql:func-desc:`ext::postgis::shiftlongitude`

    * - :eql:func:`ext::postgis::snap`
      - :eql:func-desc:`ext::postgis::snap`

    * - :eql:func:`ext::postgis::snaptogrid`
      - :eql:func-desc:`ext::postgis::snaptogrid`

    * - :eql:func:`ext::postgis::wrapx`
      - :eql:func-desc:`ext::postgis::wrapx`

Geometry Validation
-------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::isvalid`
      - :eql:func-desc:`ext::postgis::isvalid`

    * - :eql:func:`ext::postgis::isvalidreason`
      - :eql:func-desc:`ext::postgis::isvalidreason`

    * - :eql:func:`ext::postgis::makevalid`
      - :eql:func-desc:`ext::postgis::makevalid`

Spatial Reference System Functions
----------------------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::inversetransformpipeline`
      - :eql:func-desc:`ext::postgis::inversetransformpipeline`

    * - :eql:func:`ext::postgis::postgis_srs_codes`
      - :eql:func-desc:`ext::postgis::postgis_srs_codes`

    * - :eql:func:`ext::postgis::setsrid`
      - :eql:func-desc:`ext::postgis::setsrid`

    * - :eql:func:`ext::postgis::srid`
      - :eql:func-desc:`ext::postgis::srid`

    * - :eql:func:`ext::postgis::transform`
      - :eql:func-desc:`ext::postgis::transform`

    * - :eql:func:`ext::postgis::transformpipeline`
      - :eql:func-desc:`ext::postgis::transformpipeline`

Well-Known Text (WKT)
---------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::asewkt`
      - :eql:func-desc:`ext::postgis::asewkt`

    * - :eql:func:`ext::postgis::astext`
      - :eql:func-desc:`ext::postgis::astext`

    * - :eql:func:`ext::postgis::bdmpolyfromtext`
      - :eql:func-desc:`ext::postgis::bdmpolyfromtext`

    * - :eql:func:`ext::postgis::bdpolyfromtext`
      - :eql:func-desc:`ext::postgis::bdpolyfromtext`

    * - :eql:func:`ext::postgis::geogfromtext`
      - :eql:func-desc:`ext::postgis::geogfromtext`

    * - :eql:func:`ext::postgis::geomcollfromtext`
      - :eql:func-desc:`ext::postgis::geomcollfromtext`

    * - :eql:func:`ext::postgis::geomfromewkt`
      - :eql:func-desc:`ext::postgis::geomfromewkt`

    * - :eql:func:`ext::postgis::geomfrommarc21`
      - :eql:func-desc:`ext::postgis::geomfrommarc21`

    * - :eql:func:`ext::postgis::geomfromtext`
      - :eql:func-desc:`ext::postgis::geomfromtext`

    * - :eql:func:`ext::postgis::linefromtext`
      - :eql:func-desc:`ext::postgis::linefromtext`

    * - :eql:func:`ext::postgis::mlinefromtext`
      - :eql:func-desc:`ext::postgis::mlinefromtext`

    * - :eql:func:`ext::postgis::mpointfromtext`
      - :eql:func-desc:`ext::postgis::mpointfromtext`

    * - :eql:func:`ext::postgis::mpolyfromtext`
      - :eql:func-desc:`ext::postgis::mpolyfromtext`

    * - :eql:func:`ext::postgis::pointfromtext`
      - :eql:func-desc:`ext::postgis::pointfromtext`

    * - :eql:func:`ext::postgis::polygonfromtext`
      - :eql:func-desc:`ext::postgis::polygonfromtext`

Well-Known Binary (WKB)
-----------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::asbinary`
      - :eql:func-desc:`ext::postgis::asbinary`

    * - :eql:func:`ext::postgis::asewkb`
      - :eql:func-desc:`ext::postgis::asewkb`

    * - :eql:func:`ext::postgis::ashexewkb`
      - :eql:func-desc:`ext::postgis::ashexewkb`

    * - :eql:func:`ext::postgis::geogfromwkb`
      - :eql:func-desc:`ext::postgis::geogfromwkb`

    * - :eql:func:`ext::postgis::geomfromewkb`
      - :eql:func-desc:`ext::postgis::geomfromewkb`

    * - :eql:func:`ext::postgis::geomfromwkb`
      - :eql:func-desc:`ext::postgis::geomfromwkb`

    * - :eql:func:`ext::postgis::linefromwkb`
      - :eql:func-desc:`ext::postgis::linefromwkb`

    * - :eql:func:`ext::postgis::linestringfromwkb`
      - :eql:func-desc:`ext::postgis::linestringfromwkb`

    * - :eql:func:`ext::postgis::pointfromwkb`
      - :eql:func-desc:`ext::postgis::pointfromwkb`

Other Formats
-------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::asencodedpolyline`
      - :eql:func-desc:`ext::postgis::asencodedpolyline`

    * - :eql:func:`ext::postgis::asgeojson`
      - :eql:func-desc:`ext::postgis::asgeojson`

    * - :eql:func:`ext::postgis::asgml`
      - :eql:func-desc:`ext::postgis::asgml`

    * - :eql:func:`ext::postgis::askml`
      - :eql:func-desc:`ext::postgis::askml`

    * - :eql:func:`ext::postgis::aslatlontext`
      - :eql:func-desc:`ext::postgis::aslatlontext`

    * - :eql:func:`ext::postgis::asmarc21`
      - :eql:func-desc:`ext::postgis::asmarc21`

    * - :eql:func:`ext::postgis::asmvtgeom`
      - :eql:func-desc:`ext::postgis::asmvtgeom`

    * - :eql:func:`ext::postgis::assvg`
      - :eql:func-desc:`ext::postgis::assvg`

    * - :eql:func:`ext::postgis::astwkb`
      - :eql:func-desc:`ext::postgis::astwkb`

    * - :eql:func:`ext::postgis::asx3d`
      - :eql:func-desc:`ext::postgis::asx3d`

    * - :eql:func:`ext::postgis::box2dfromgeohash`
      - :eql:func-desc:`ext::postgis::box2dfromgeohash`

    * - :eql:func:`ext::postgis::geohash`
      - :eql:func-desc:`ext::postgis::geohash`

    * - :eql:func:`ext::postgis::geomfromgeohash`
      - :eql:func-desc:`ext::postgis::geomfromgeohash`

    * - :eql:func:`ext::postgis::geomfromgeojson`
      - :eql:func-desc:`ext::postgis::geomfromgeojson`

    * - :eql:func:`ext::postgis::geomfromgml`
      - :eql:func-desc:`ext::postgis::geomfromgml`

    * - :eql:func:`ext::postgis::geomfromkml`
      - :eql:func-desc:`ext::postgis::geomfromkml`

    * - :eql:func:`ext::postgis::geomfromtwkb`
      - :eql:func-desc:`ext::postgis::geomfromtwkb`

    * - :eql:func:`ext::postgis::linefromencodedpolyline`
      - :eql:func-desc:`ext::postgis::linefromencodedpolyline`

    * - :eql:func:`ext::postgis::pointfromgeohash`
      - :eql:func-desc:`ext::postgis::pointfromgeohash`

Topological Relationships
-------------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::contains`
      - :eql:func-desc:`ext::postgis::contains`

    * - :eql:func:`ext::postgis::containsproperly`
      - :eql:func-desc:`ext::postgis::containsproperly`

    * - :eql:func:`ext::postgis::coveredby`
      - :eql:func-desc:`ext::postgis::coveredby`

    * - :eql:func:`ext::postgis::covers`
      - :eql:func-desc:`ext::postgis::covers`

    * - :eql:func:`ext::postgis::crosses`
      - :eql:func-desc:`ext::postgis::crosses`

    * - :eql:func:`ext::postgis::disjoint`
      - :eql:func-desc:`ext::postgis::disjoint`

    * - :eql:func:`ext::postgis::equals`
      - :eql:func-desc:`ext::postgis::equals`

    * - :eql:func:`ext::postgis::intersects`
      - :eql:func-desc:`ext::postgis::intersects`

    * - :eql:func:`ext::postgis::intersects3d`
      - :eql:func-desc:`ext::postgis::intersects3d`

    * - :eql:func:`ext::postgis::linecrossingdirection`
      - :eql:func-desc:`ext::postgis::linecrossingdirection`

    * - :eql:func:`ext::postgis::orderingequals`
      - :eql:func-desc:`ext::postgis::orderingequals`

    * - :eql:func:`ext::postgis::overlaps`
      - :eql:func-desc:`ext::postgis::overlaps`

    * - :eql:func:`ext::postgis::relate`
      - :eql:func-desc:`ext::postgis::relate`

    * - :eql:func:`ext::postgis::relatematch`
      - :eql:func-desc:`ext::postgis::relatematch`

    * - :eql:func:`ext::postgis::touches`
      - :eql:func-desc:`ext::postgis::touches`

    * - :eql:func:`ext::postgis::within`
      - :eql:func-desc:`ext::postgis::within`

Distance Relationships
----------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::dfullywithin`
      - :eql:func-desc:`ext::postgis::dfullywithin`

    * - :eql:func:`ext::postgis::dfullywithin3d`
      - :eql:func-desc:`ext::postgis::dfullywithin3d`

    * - :eql:func:`ext::postgis::dwithin`
      - :eql:func-desc:`ext::postgis::dwithin`

    * - :eql:func:`ext::postgis::dwithin3d`
      - :eql:func-desc:`ext::postgis::dwithin3d`

    * - :eql:func:`ext::postgis::pointinsidecircle`
      - :eql:func-desc:`ext::postgis::pointinsidecircle`

Measurement Functions
---------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::angle`
      - :eql:func-desc:`ext::postgis::angle`

    * - :eql:func:`ext::postgis::area`
      - :eql:func-desc:`ext::postgis::area`

    * - :eql:func:`ext::postgis::azimuth`
      - :eql:func-desc:`ext::postgis::azimuth`

    * - :eql:func:`ext::postgis::closestpoint`
      - :eql:func-desc:`ext::postgis::closestpoint`

    * - :eql:func:`ext::postgis::closestpoint3d`
      - :eql:func-desc:`ext::postgis::closestpoint3d`

    * - :eql:func:`ext::postgis::distance`
      - :eql:func-desc:`ext::postgis::distance`

    * - :eql:func:`ext::postgis::distance3d`
      - :eql:func-desc:`ext::postgis::distance3d`

    * - :eql:func:`ext::postgis::distancesphere`
      - :eql:func-desc:`ext::postgis::distancesphere`

    * - :eql:func:`ext::postgis::distancespheroid`
      - :eql:func-desc:`ext::postgis::distancespheroid`

    * - :eql:func:`ext::postgis::frechetdistance`
      - :eql:func-desc:`ext::postgis::frechetdistance`

    * - :eql:func:`ext::postgis::hausdorffdistance`
      - :eql:func-desc:`ext::postgis::hausdorffdistance`

    * - :eql:func:`ext::postgis::length`
      - :eql:func-desc:`ext::postgis::length`

    * - :eql:func:`ext::postgis::length2d`
      - :eql:func-desc:`ext::postgis::length2d`

    * - :eql:func:`ext::postgis::length3d`
      - :eql:func-desc:`ext::postgis::length3d`

    * - :eql:func:`ext::postgis::longestline`
      - :eql:func-desc:`ext::postgis::longestline`

    * - :eql:func:`ext::postgis::longestline3d`
      - :eql:func-desc:`ext::postgis::longestline3d`

    * - :eql:func:`ext::postgis::maxdistance`
      - :eql:func-desc:`ext::postgis::maxdistance`

    * - :eql:func:`ext::postgis::maxdistance3d`
      - :eql:func-desc:`ext::postgis::maxdistance3d`

    * - :eql:func:`ext::postgis::minimumclearance`
      - :eql:func-desc:`ext::postgis::minimumclearance`

    * - :eql:func:`ext::postgis::minimumclearanceline`
      - :eql:func-desc:`ext::postgis::minimumclearanceline`

    * - :eql:func:`ext::postgis::perimeter`
      - :eql:func-desc:`ext::postgis::perimeter`

    * - :eql:func:`ext::postgis::perimeter2d`
      - :eql:func-desc:`ext::postgis::perimeter2d`

    * - :eql:func:`ext::postgis::perimeter3d`
      - :eql:func-desc:`ext::postgis::perimeter3d`

    * - :eql:func:`ext::postgis::shortestline`
      - :eql:func-desc:`ext::postgis::shortestline`

    * - :eql:func:`ext::postgis::shortestline3d`
      - :eql:func-desc:`ext::postgis::shortestline3d`

Overlay Functions
-----------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::clipbybox2d`
      - :eql:func-desc:`ext::postgis::clipbybox2d`

    * - :eql:func:`ext::postgis::difference`
      - :eql:func-desc:`ext::postgis::difference`

    * - :eql:func:`ext::postgis::intersection`
      - :eql:func-desc:`ext::postgis::intersection`

    * - :eql:func:`ext::postgis::node`
      - :eql:func-desc:`ext::postgis::node`

    * - :eql:func:`ext::postgis::split`
      - :eql:func-desc:`ext::postgis::split`

    * - :eql:func:`ext::postgis::subdivide`
      - :eql:func-desc:`ext::postgis::subdivide`

    * - :eql:func:`ext::postgis::symdifference`
      - :eql:func-desc:`ext::postgis::symdifference`

    * - :eql:func:`ext::postgis::unaryunion`
      - :eql:func-desc:`ext::postgis::unaryunion`

    * - :eql:func:`ext::postgis::union`
      - :eql:func-desc:`ext::postgis::union`

Geometry Processing
-------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::buffer`
      - :eql:func-desc:`ext::postgis::buffer`

    * - :eql:func:`ext::postgis::buildarea`
      - :eql:func-desc:`ext::postgis::buildarea`

    * - :eql:func:`ext::postgis::centroid`
      - :eql:func-desc:`ext::postgis::centroid`

    * - :eql:func:`ext::postgis::chaikinsmoothing`
      - :eql:func-desc:`ext::postgis::chaikinsmoothing`

    * - :eql:func:`ext::postgis::concavehull`
      - :eql:func-desc:`ext::postgis::concavehull`

    * - :eql:func:`ext::postgis::convexhull`
      - :eql:func-desc:`ext::postgis::convexhull`

    * - :eql:func:`ext::postgis::delaunaytriangles`
      - :eql:func-desc:`ext::postgis::delaunaytriangles`

    * - :eql:func:`ext::postgis::filterbym`
      - :eql:func-desc:`ext::postgis::filterbym`

    * - :eql:func:`ext::postgis::generatepoints`
      - :eql:func-desc:`ext::postgis::generatepoints`

    * - :eql:func:`ext::postgis::geometricmedian`
      - :eql:func-desc:`ext::postgis::geometricmedian`

    * - :eql:func:`ext::postgis::linemerge`
      - :eql:func-desc:`ext::postgis::linemerge`

    * - :eql:func:`ext::postgis::minimumboundingcircle`
      - :eql:func-desc:`ext::postgis::minimumboundingcircle`

    * - :eql:func:`ext::postgis::offsetcurve`
      - :eql:func-desc:`ext::postgis::offsetcurve`

    * - :eql:func:`ext::postgis::orientedenvelope`
      - :eql:func-desc:`ext::postgis::orientedenvelope`

    * - :eql:func:`ext::postgis::pointonsurface`
      - :eql:func-desc:`ext::postgis::pointonsurface`

    * - :eql:func:`ext::postgis::polygonize`
      - :eql:func-desc:`ext::postgis::polygonize`

    * - :eql:func:`ext::postgis::reduceprecision`
      - :eql:func-desc:`ext::postgis::reduceprecision`

    * - :eql:func:`ext::postgis::seteffectivearea`
      - :eql:func-desc:`ext::postgis::seteffectivearea`

    * - :eql:func:`ext::postgis::sharedpaths`
      - :eql:func-desc:`ext::postgis::sharedpaths`

    * - :eql:func:`ext::postgis::simplify`
      - :eql:func-desc:`ext::postgis::simplify`

    * - :eql:func:`ext::postgis::simplifypolygonhull`
      - :eql:func-desc:`ext::postgis::simplifypolygonhull`

    * - :eql:func:`ext::postgis::simplifypreservetopology`
      - :eql:func-desc:`ext::postgis::simplifypreservetopology`

    * - :eql:func:`ext::postgis::simplifyvw`
      - :eql:func-desc:`ext::postgis::simplifyvw`

    * - :eql:func:`ext::postgis::triangulatepolygon`
      - :eql:func-desc:`ext::postgis::triangulatepolygon`

    * - :eql:func:`ext::postgis::voronoilines`
      - :eql:func-desc:`ext::postgis::voronoilines`

    * - :eql:func:`ext::postgis::voronoipolygons`
      - :eql:func-desc:`ext::postgis::voronoipolygons`

Coverages
---------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::coverageunion`
      - :eql:func-desc:`ext::postgis::coverageunion`

Affine Transformations
----------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::affine`
      - :eql:func-desc:`ext::postgis::affine`

    * - :eql:func:`ext::postgis::rotate`
      - :eql:func-desc:`ext::postgis::rotate`

    * - :eql:func:`ext::postgis::rotatex`
      - :eql:func-desc:`ext::postgis::rotatex`

    * - :eql:func:`ext::postgis::rotatey`
      - :eql:func-desc:`ext::postgis::rotatey`

    * - :eql:func:`ext::postgis::rotatez`
      - :eql:func-desc:`ext::postgis::rotatez`

    * - :eql:func:`ext::postgis::scale`
      - :eql:func-desc:`ext::postgis::scale`

    * - :eql:func:`ext::postgis::translate`
      - :eql:func-desc:`ext::postgis::translate`

    * - :eql:func:`ext::postgis::transscale`
      - :eql:func-desc:`ext::postgis::transscale`

Clustering Functions
--------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::clusterintersecting`
      - :eql:func-desc:`ext::postgis::clusterintersecting`

    * - :eql:func:`ext::postgis::clusterwithin`
      - :eql:func-desc:`ext::postgis::clusterwithin`

Bounding Box Functions
----------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::expand`
      - :eql:func-desc:`ext::postgis::expand`

    * - :eql:func:`ext::postgis::makebox2d`
      - :eql:func-desc:`ext::postgis::makebox2d`

    * - :eql:func:`ext::postgis::makebox3d`
      - :eql:func-desc:`ext::postgis::makebox3d`

    * - :eql:func:`ext::postgis::to_box2d`
      - :eql:func-desc:`ext::postgis::to_box2d`

    * - :eql:func:`ext::postgis::to_box3d`
      - :eql:func-desc:`ext::postgis::to_box3d`

    * - :eql:func:`ext::postgis::xmax`
      - :eql:func-desc:`ext::postgis::xmax`

    * - :eql:func:`ext::postgis::xmin`
      - :eql:func-desc:`ext::postgis::xmin`

    * - :eql:func:`ext::postgis::ymax`
      - :eql:func-desc:`ext::postgis::ymax`

    * - :eql:func:`ext::postgis::ymin`
      - :eql:func-desc:`ext::postgis::ymin`

    * - :eql:func:`ext::postgis::zmax`
      - :eql:func-desc:`ext::postgis::zmax`

    * - :eql:func:`ext::postgis::zmin`
      - :eql:func-desc:`ext::postgis::zmin`

Linear Referencing
------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::addmeasure`
      - :eql:func-desc:`ext::postgis::addmeasure`

    * - :eql:func:`ext::postgis::interpolatepoint`
      - :eql:func-desc:`ext::postgis::interpolatepoint`

    * - :eql:func:`ext::postgis::lineinterpolatepoint`
      - :eql:func-desc:`ext::postgis::lineinterpolatepoint`

    * - :eql:func:`ext::postgis::lineinterpolatepoint3d`
      - :eql:func-desc:`ext::postgis::lineinterpolatepoint3d`

    * - :eql:func:`ext::postgis::lineinterpolatepoints`
      - :eql:func-desc:`ext::postgis::lineinterpolatepoints`

    * - :eql:func:`ext::postgis::linelocatepoint`
      - :eql:func-desc:`ext::postgis::linelocatepoint`

    * - :eql:func:`ext::postgis::linesubstring`
      - :eql:func-desc:`ext::postgis::linesubstring`

    * - :eql:func:`ext::postgis::locatealong`
      - :eql:func-desc:`ext::postgis::locatealong`

    * - :eql:func:`ext::postgis::locatebetween`
      - :eql:func-desc:`ext::postgis::locatebetween`

    * - :eql:func:`ext::postgis::locatebetweenelevations`
      - :eql:func-desc:`ext::postgis::locatebetweenelevations`

Trajectory Functions
--------------------

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::closestpointofapproach`
      - :eql:func-desc:`ext::postgis::closestpointofapproach`

    * - :eql:func:`ext::postgis::cpawithin`
      - :eql:func-desc:`ext::postgis::cpawithin`

    * - :eql:func:`ext::postgis::distancecpa`
      - :eql:func-desc:`ext::postgis::distancecpa`

    * - :eql:func:`ext::postgis::isvalidtrajectory`
      - :eql:func-desc:`ext::postgis::isvalidtrajectory`

Other
-----

.. list-table::
    :class: funcoptable

    * - :eql:func:`ext::postgis::area2d`
      - :eql:func-desc:`ext::postgis::area2d`

    * - :eql:func:`ext::postgis::cleangeometry`
      - :eql:func-desc:`ext::postgis::cleangeometry`

    * - :eql:func:`ext::postgis::combinebbox`
      - :eql:func-desc:`ext::postgis::combinebbox`

    * - :eql:func:`ext::postgis::curven`
      - :eql:func-desc:`ext::postgis::curven`

    * - :eql:func:`ext::postgis::geography_cmp`
      - :eql:func-desc:`ext::postgis::geography_cmp`

    * - :eql:func:`ext::postgis::geomcollfromwkb`
      - :eql:func-desc:`ext::postgis::geomcollfromwkb`

    * - :eql:func:`ext::postgis::geometry_cmp`
      - :eql:func-desc:`ext::postgis::geometry_cmp`

    * - :eql:func:`ext::postgis::geometry_hash`
      - :eql:func-desc:`ext::postgis::geometry_hash`

    * - :eql:func:`ext::postgis::get_proj4_from_srid`
      - :eql:func-desc:`ext::postgis::get_proj4_from_srid`

    * - :eql:func:`ext::postgis::hasm`
      - :eql:func-desc:`ext::postgis::hasm`

    * - :eql:func:`ext::postgis::hasz`
      - :eql:func-desc:`ext::postgis::hasz`

    * - :eql:func:`ext::postgis::mlinefromwkb`
      - :eql:func-desc:`ext::postgis::mlinefromwkb`

    * - :eql:func:`ext::postgis::mpointfromwkb`
      - :eql:func-desc:`ext::postgis::mpointfromwkb`

    * - :eql:func:`ext::postgis::mpolyfromwkb`
      - :eql:func-desc:`ext::postgis::mpolyfromwkb`

    * - :eql:func:`ext::postgis::multilinefromwkb`
      - :eql:func-desc:`ext::postgis::multilinefromwkb`

    * - :eql:func:`ext::postgis::multilinestringfromtext`
      - :eql:func-desc:`ext::postgis::multilinestringfromtext`

    * - :eql:func:`ext::postgis::multipointfromtext`
      - :eql:func-desc:`ext::postgis::multipointfromtext`

    * - :eql:func:`ext::postgis::multipointfromwkb`
      - :eql:func-desc:`ext::postgis::multipointfromwkb`

    * - :eql:func:`ext::postgis::multipolyfromwkb`
      - :eql:func-desc:`ext::postgis::multipolyfromwkb`

    * - :eql:func:`ext::postgis::multipolygonfromtext`
      - :eql:func-desc:`ext::postgis::multipolygonfromtext`

    * - :eql:func:`ext::postgis::numcurves`
      - :eql:func-desc:`ext::postgis::numcurves`

    * - :eql:func:`ext::postgis::polyfromtext`
      - :eql:func-desc:`ext::postgis::polyfromtext`

    * - :eql:func:`ext::postgis::polyfromwkb`
      - :eql:func-desc:`ext::postgis::polyfromwkb`

    * - :eql:func:`ext::postgis::polygonfromwkb`
      - :eql:func-desc:`ext::postgis::polygonfromwkb`

    * - :eql:func:`ext::postgis::postgis_addbbox`
      - :eql:func-desc:`ext::postgis::postgis_addbbox`

    * - :eql:func:`ext::postgis::postgis_constraint_dims`
      - :eql:func-desc:`ext::postgis::postgis_constraint_dims`

    * - :eql:func:`ext::postgis::postgis_constraint_srid`
      - :eql:func-desc:`ext::postgis::postgis_constraint_srid`

    * - :eql:func:`ext::postgis::postgis_dropbbox`
      - :eql:func-desc:`ext::postgis::postgis_dropbbox`

    * - :eql:func:`ext::postgis::postgis_full_version`
      - :eql:func-desc:`ext::postgis::postgis_full_version`

    * - :eql:func:`ext::postgis::postgis_geos_compiled_version`
      - :eql:func-desc:`ext::postgis::postgis_geos_compiled_version`

    * - :eql:func:`ext::postgis::postgis_geos_noop`
      - :eql:func-desc:`ext::postgis::postgis_geos_noop`

    * - :eql:func:`ext::postgis::postgis_geos_version`
      - :eql:func-desc:`ext::postgis::postgis_geos_version`

    * - :eql:func:`ext::postgis::postgis_getbbox`
      - :eql:func-desc:`ext::postgis::postgis_getbbox`

    * - :eql:func:`ext::postgis::postgis_hasbbox`
      - :eql:func-desc:`ext::postgis::postgis_hasbbox`

    * - :eql:func:`ext::postgis::postgis_lib_build_date`
      - :eql:func-desc:`ext::postgis::postgis_lib_build_date`

    * - :eql:func:`ext::postgis::postgis_lib_revision`
      - :eql:func-desc:`ext::postgis::postgis_lib_revision`

    * - :eql:func:`ext::postgis::postgis_lib_version`
      - :eql:func-desc:`ext::postgis::postgis_lib_version`

    * - :eql:func:`ext::postgis::postgis_libjson_version`
      - :eql:func-desc:`ext::postgis::postgis_libjson_version`

    * - :eql:func:`ext::postgis::postgis_liblwgeom_version`
      - :eql:func-desc:`ext::postgis::postgis_liblwgeom_version`

    * - :eql:func:`ext::postgis::postgis_libprotobuf_version`
      - :eql:func-desc:`ext::postgis::postgis_libprotobuf_version`

    * - :eql:func:`ext::postgis::postgis_libxml_version`
      - :eql:func-desc:`ext::postgis::postgis_libxml_version`

    * - :eql:func:`ext::postgis::postgis_noop`
      - :eql:func-desc:`ext::postgis::postgis_noop`

    * - :eql:func:`ext::postgis::postgis_proj_compiled_version`
      - :eql:func-desc:`ext::postgis::postgis_proj_compiled_version`

    * - :eql:func:`ext::postgis::postgis_proj_version`
      - :eql:func-desc:`ext::postgis::postgis_proj_version`

    * - :eql:func:`ext::postgis::postgis_scripts_build_date`
      - :eql:func-desc:`ext::postgis::postgis_scripts_build_date`

    * - :eql:func:`ext::postgis::postgis_scripts_installed`
      - :eql:func-desc:`ext::postgis::postgis_scripts_installed`

    * - :eql:func:`ext::postgis::postgis_scripts_released`
      - :eql:func-desc:`ext::postgis::postgis_scripts_released`

    * - :eql:func:`ext::postgis::postgis_svn_version`
      - :eql:func-desc:`ext::postgis::postgis_svn_version`

    * - :eql:func:`ext::postgis::postgis_transform_geometry`
      - :eql:func-desc:`ext::postgis::postgis_transform_geometry`

    * - :eql:func:`ext::postgis::postgis_transform_pipeline_geometry`
      - :eql:func-desc:`ext::postgis::postgis_transform_pipeline_geometry`

    * - :eql:func:`ext::postgis::postgis_typmod_dims`
      - :eql:func-desc:`ext::postgis::postgis_typmod_dims`

    * - :eql:func:`ext::postgis::postgis_typmod_srid`
      - :eql:func-desc:`ext::postgis::postgis_typmod_srid`

    * - :eql:func:`ext::postgis::postgis_typmod_type`
      - :eql:func-desc:`ext::postgis::postgis_typmod_type`

    * - :eql:func:`ext::postgis::postgis_version`
      - :eql:func-desc:`ext::postgis::postgis_version`

    * - :eql:func:`ext::postgis::postgis_wagyu_version`
      - :eql:func-desc:`ext::postgis::postgis_wagyu_version`

    * - :eql:func:`ext::postgis::removeirrelevantpointsforview`
      - :eql:func-desc:`ext::postgis::removeirrelevantpointsforview`

    * - :eql:func:`ext::postgis::removesmallparts`
      - :eql:func-desc:`ext::postgis::removesmallparts`

    * - :eql:func:`ext::postgis::symmetricdifference`
      - :eql:func-desc:`ext::postgis::symmetricdifference`

    * - :eql:func:`ext::postgis::to_geography`
      - :eql:func-desc:`ext::postgis::to_geography`

    * - :eql:func:`ext::postgis::to_geometry`
      - :eql:func-desc:`ext::postgis::to_geometry`

----------


.. eql:function:: ext::postgis::addmeasure( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_addmeasure``.


----------


.. eql:function:: ext::postgis::addpoint( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::addpoint( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_addpoint``.


----------


.. eql:function:: ext::postgis::affine( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                    a4: std::float64, \
                    a5: std::float64, \
                    a6: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::affine( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                    a4: std::float64, \
                    a5: std::float64, \
                    a6: std::float64, \
                    a7: std::float64, \
                    a8: std::float64, \
                    a9: std::float64, \
                    a10: std::float64, \
                    a11: std::float64, \
                    a12: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_affine``.


----------


.. eql:function:: ext::postgis::angle( \
                    line1: ext::postgis::geometry, \
                    line2: ext::postgis::geometry, \
                  ) ->  std::float64
                  ext::postgis::angle( \
                    pt1: ext::postgis::geometry, \
                    pt2: ext::postgis::geometry, \
                    pt3: ext::postgis::geometry, \
                    pt4: ext::postgis::geometry = <ext::postgis::geometry>'POINT EMPTY', \
                  ) ->  std::float64

    This is exposing ``st_angle``.


----------


.. eql:function:: ext::postgis::area( \
                    a0: std::str \
                  ) ->  std::float64
                  ext::postgis::area( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64
                  ext::postgis::area( \
                    geog: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  std::float64

    This is exposing ``st_area``.


----------


.. eql:function:: ext::postgis::area2d( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_area2d``.


----------


.. eql:function:: ext::postgis::asbinary( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bytes
                  ext::postgis::asbinary( \
                    a0: ext::postgis::geography \
                  ) ->  std::bytes
                  ext::postgis::asbinary( \
                    a0: ext::postgis::geometry, \
                    a1: std::str, \
                  ) ->  std::bytes
                  ext::postgis::asbinary( \
                    a0: optional ext::postgis::geography, \
                    a1: optional std::str, \
                  ) -> optional std::bytes

    Returns a geometry/geography in WKB format without SRID meta data.

    Returns the OGC/ISO Well-Known Binary (WKB) representation of the
    geometry/geography without SRID meta data.


    This is exposing ``st_asbinary``.


----------


.. eql:function:: ext::postgis::asencodedpolyline( \
                    geom: ext::postgis::geometry, \
                    nprecision: std::int64 = 5, \
                  ) ->  std::str

    This is exposing ``st_asencodedpolyline``.


----------


.. eql:function:: ext::postgis::asewkb( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bytes
                  ext::postgis::asewkb( \
                    a0: ext::postgis::geometry, \
                    a1: std::str, \
                  ) ->  std::bytes

    Returns a geometry in EWKB format with SRID meta data.

    Returns the Extended Well-Known Binary (EWKB) representation of the
    geometry with SRID meta data.


    This is exposing ``st_asewkb``.


----------


.. eql:function:: ext::postgis::asewkt( \
                    a0: std::str \
                  ) ->  std::str
                  ext::postgis::asewkt( \
                    a0: ext::postgis::geometry \
                  ) ->  std::str
                  ext::postgis::asewkt( \
                    a0: ext::postgis::geography \
                  ) ->  std::str
                  ext::postgis::asewkt( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  std::str
                  ext::postgis::asewkt( \
                    a0: ext::postgis::geography, \
                    a1: std::int64, \
                  ) ->  std::str

    Returns a geometry in WKT format with SRID meta data.

    Returns the Well-Known Text (WKT) representation of the geometry with SRID
    meta data.


    This is exposing ``st_asewkt``.


----------


.. eql:function:: ext::postgis::asgeojson( \
                    a0: std::str \
                  ) ->  std::str
                  ext::postgis::asgeojson( \
                    geom: ext::postgis::geometry, \
                    maxdecimaldigits: std::int64 = 9, \
                    options: std::int64 = 8, \
                  ) ->  std::str
                  ext::postgis::asgeojson( \
                    geog: ext::postgis::geography, \
                    maxdecimaldigits: std::int64 = 9, \
                    options: std::int64 = 0, \
                  ) ->  std::str

    This is exposing ``st_asgeojson``.


----------


.. eql:function:: ext::postgis::asgml( \
                    a0: std::str \
                  ) ->  std::str
                  ext::postgis::asgml( \
                    geom: optional ext::postgis::geometry, \
                    maxdecimaldigits: optional std::int64 = 15, \
                    options: optional std::int64 = 0, \
                  ) -> optional std::str
                  ext::postgis::asgml( \
                    geog: ext::postgis::geography, \
                    maxdecimaldigits: std::int64 = 15, \
                    options: std::int64 = 0, \
                    nprefix: std::str = 'gml', \
                    id: std::str = '', \
                  ) ->  std::str
                  ext::postgis::asgml( \
                    version: std::int64, \
                    geog: ext::postgis::geography, \
                    maxdecimaldigits: std::int64 = 15, \
                    options: std::int64 = 0, \
                    nprefix: std::str = 'gml', \
                    id: std::str = '', \
                  ) ->  std::str
                  ext::postgis::asgml( \
                    version: optional std::int64, \
                    geom: optional ext::postgis::geometry, \
                    maxdecimaldigits: optional std::int64 = 15, \
                    options: optional std::int64 = 0, \
                    nprefix: optional std::str = {}, \
                    id: optional std::str = {}, \
                  ) -> optional std::str

    This is exposing ``st_asgml``.


----------


.. eql:function:: ext::postgis::ashexewkb( \
                    a0: ext::postgis::geometry \
                  ) ->  std::str
                  ext::postgis::ashexewkb( \
                    a0: ext::postgis::geometry, \
                    a1: std::str, \
                  ) ->  std::str

    Returns a geometry in HEXEWKB format (as text).

    Returnss a geometry in HEXEWKB format (as text) using either little-endian
    (NDR) or big-endian (XDR) encoding.


    This is exposing ``st_ashexewkb``.


----------


.. eql:function:: ext::postgis::askml( \
                    a0: std::str \
                  ) ->  std::str
                  ext::postgis::askml( \
                    geom: ext::postgis::geometry, \
                    maxdecimaldigits: std::int64 = 15, \
                    nprefix: std::str = '', \
                  ) ->  std::str
                  ext::postgis::askml( \
                    geog: ext::postgis::geography, \
                    maxdecimaldigits: std::int64 = 15, \
                    nprefix: std::str = '', \
                  ) ->  std::str

    This is exposing ``st_askml``.


----------


.. eql:function:: ext::postgis::aslatlontext( \
                    geom: ext::postgis::geometry, \
                    tmpl: std::str = '', \
                  ) ->  std::str

    This is exposing ``st_aslatlontext``.


----------


.. eql:function:: ext::postgis::asmarc21( \
                    geom: ext::postgis::geometry, \
                    format: std::str = 'hdddmmss', \
                  ) ->  std::str

    This is exposing ``st_asmarc21``.


----------


.. eql:function:: ext::postgis::asmvtgeom( \
                    geom: optional ext::postgis::geometry, \
                    bounds: optional ext::postgis::box2d, \
                    extent: optional std::int64 = 4096, \
                    buffer: optional std::int64 = 256, \
                    clip_geom: optional std::bool = true, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_asmvtgeom``.


----------


.. eql:function:: ext::postgis::assvg( \
                    a0: std::str \
                  ) ->  std::str
                  ext::postgis::assvg( \
                    geom: ext::postgis::geometry, \
                    rel: std::int64 = 0, \
                    maxdecimaldigits: std::int64 = 15, \
                  ) ->  std::str
                  ext::postgis::assvg( \
                    geog: ext::postgis::geography, \
                    rel: std::int64 = 0, \
                    maxdecimaldigits: std::int64 = 15, \
                  ) ->  std::str

    This is exposing ``st_assvg``.


----------


.. eql:function:: ext::postgis::astext( \
                    a0: std::str \
                  ) ->  std::str
                  ext::postgis::astext( \
                    a0: ext::postgis::geometry \
                  ) ->  std::str
                  ext::postgis::astext( \
                    a0: ext::postgis::geography \
                  ) ->  std::str
                  ext::postgis::astext( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  std::str
                  ext::postgis::astext( \
                    a0: ext::postgis::geography, \
                    a1: std::int64, \
                  ) ->  std::str

    Returns a geometry/geography in WKT format without SRID metadata.

    Returns the Well-Known Text (WKT) representation of the geometry/geography
    without SRID metadata.


    This is exposing ``st_astext``.


----------


.. eql:function:: ext::postgis::astwkb( \
                    geom: optional ext::postgis::geometry, \
                    prec: optional std::int64 = {}, \
                    prec_z: optional std::int64 = {}, \
                    prec_m: optional std::int64 = {}, \
                    with_sizes: optional std::bool = {}, \
                    with_boxes: optional std::bool = {}, \
                  ) -> optional std::bytes
                  ext::postgis::astwkb( \
                    geom: optional array<ext::postgis::geometry>, \
                    ids: optional array<std::int64>, \
                    prec: optional std::int64 = {}, \
                    prec_z: optional std::int64 = {}, \
                    prec_m: optional std::int64 = {}, \
                    with_sizes: optional std::bool = {}, \
                    with_boxes: optional std::bool = {}, \
                  ) -> optional std::bytes

    This is exposing ``st_astwkb``.


----------


.. eql:function:: ext::postgis::asx3d( \
                    geom: optional ext::postgis::geometry, \
                    maxdecimaldigits: optional std::int64 = 15, \
                    options: optional std::int64 = 0, \
                  ) -> optional std::str

    Returns a geometry in X3D format.

    Returns a geometry in X3D xml node element format:
    ISO-IEC-19776-1.2-X3DEncodings-XML.


    This is exposing ``st_asx3d``.


----------


.. eql:function:: ext::postgis::azimuth( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64
                  ext::postgis::azimuth( \
                    geog1: ext::postgis::geography, \
                    geog2: ext::postgis::geography, \
                  ) ->  std::float64

    This is exposing ``st_azimuth``.


----------


.. eql:function:: ext::postgis::bdmpolyfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_bdmpolyfromtext``.


----------


.. eql:function:: ext::postgis::bdpolyfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_bdpolyfromtext``.


----------


.. eql:function:: ext::postgis::boundary( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_boundary``.


----------


.. eql:function:: ext::postgis::boundingdiagonal( \
                    geom: ext::postgis::geometry, \
                    fits: std::bool = false, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_boundingdiagonal``.


----------


.. eql:function:: ext::postgis::box2dfromgeohash( \
                    a0: optional std::str, \
                    a1: optional std::int64 = {}, \
                  ) -> optional ext::postgis::box2d

    This is exposing ``st_box2dfromgeohash``.


----------


.. eql:function:: ext::postgis::buffer( \
                    a0: std::str, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::buffer( \
                    a0: std::str, \
                    a1: std::float64, \
                    a2: std::str, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::buffer( \
                    a0: ext::postgis::geography, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geography
                  ext::postgis::buffer( \
                    a0: std::str, \
                    a1: std::float64, \
                    a2: std::int64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::buffer( \
                    a0: ext::postgis::geography, \
                    a1: std::float64, \
                    a2: std::str, \
                  ) ->  ext::postgis::geography
                  ext::postgis::buffer( \
                    a0: ext::postgis::geography, \
                    a1: std::float64, \
                    a2: std::int64, \
                  ) ->  ext::postgis::geography
                  ext::postgis::buffer( \
                    geom: ext::postgis::geometry, \
                    radius: std::float64, \
                    quadsegs: std::int64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::buffer( \
                    geom: ext::postgis::geometry, \
                    radius: std::float64, \
                    options: std::str = '', \
                  ) ->  ext::postgis::geometry

    Returns a geometry covering all points within a given distance from a
    geometry.

    This is exposing ``st_buffer``.


----------


.. eql:function:: ext::postgis::buildarea( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_buildarea``.


----------


.. eql:function:: ext::postgis::centroid( \
                    a0: std::str \
                  ) ->  ext::postgis::geometry
                  ext::postgis::centroid( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry
                  ext::postgis::centroid( \
                    a0: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  ext::postgis::geography

    This is exposing ``st_centroid``.


----------


.. eql:function:: ext::postgis::chaikinsmoothing( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64 = 1, \
                    a2: std::bool = false, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_chaikinsmoothing``.


----------


.. eql:function:: ext::postgis::cleangeometry( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_cleangeometry``.


----------


.. eql:function:: ext::postgis::clipbybox2d( \
                    geom: ext::postgis::geometry, \
                    box: ext::postgis::box2d, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_clipbybox2d``.


----------


.. eql:function:: ext::postgis::closestpoint( \
                    a0: optional std::str, \
                    a1: optional std::str, \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::closestpoint( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::closestpoint( \
                    a0: ext::postgis::geography, \
                    a1: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  ext::postgis::geography

    Returns the 2D point of the first geometry closest to the second.

    Returns the 2D point of the first geometry/geography that is closest to
    the second geometry/geography. This is the first point of the shortest
    line from one geometry to the other.


    This is exposing ``st_closestpoint``.


----------


.. eql:function:: ext::postgis::closestpoint3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    Returns the 3D point of the first geometry closest to the second.

    Returns the 3D point of the first geometry/geography that is closest to
    the second geometry/geography. This is the first point of the 3D shortest
    line.


    This is exposing ``st_3dclosestpoint``.


----------


.. eql:function:: ext::postgis::closestpointofapproach( \
                    a0: ext::postgis::geometry, \
                    a1: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing ``st_closestpointofapproach``.


----------


.. eql:function:: ext::postgis::clusterintersecting( \
                    a0: array<ext::postgis::geometry> \
                  ) ->  array<ext::postgis::geometry>

    This is exposing ``st_clusterintersecting``.


----------


.. eql:function:: ext::postgis::clusterwithin( \
                    a0: array<ext::postgis::geometry>, \
                    a1: std::float64, \
                  ) ->  array<ext::postgis::geometry>

    This is exposing ``st_clusterwithin``.


----------


.. eql:function:: ext::postgis::collect( \
                    a0: array<ext::postgis::geometry> \
                  ) ->  ext::postgis::geometry
                  ext::postgis::collect( \
                    geom1: optional ext::postgis::geometry, \
                    geom2: optional ext::postgis::geometry, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_collect``.


----------


.. eql:function:: ext::postgis::collectionextract( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry
                  ext::postgis::collectionextract( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_collectionextract``.


----------


.. eql:function:: ext::postgis::collectionhomogenize( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_collectionhomogenize``.


----------


.. eql:function:: ext::postgis::combinebbox( \
                    a0: optional ext::postgis::box3d, \
                    a1: optional ext::postgis::box3d, \
                  ) -> optional ext::postgis::box3d
                  ext::postgis::combinebbox( \
                    a0: optional ext::postgis::box2d, \
                    a1: optional ext::postgis::geometry, \
                  ) -> optional ext::postgis::box2d
                  ext::postgis::combinebbox( \
                    a0: optional ext::postgis::box3d, \
                    a1: optional ext::postgis::geometry, \
                  ) -> optional ext::postgis::box3d

    This is exposing ``st_combinebbox``.


----------


.. eql:function:: ext::postgis::concavehull( \
                    param_geom: ext::postgis::geometry, \
                    param_pctconvex: std::float64, \
                    param_allow_holes: std::bool = false, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_concavehull``.


----------


.. eql:function:: ext::postgis::contains( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing ``st_contains``.


----------


.. eql:function:: ext::postgis::containsproperly( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    Tests if every point of *geom2* lies in the interior of *geom1*.

    This is exposing ``st_containsproperly``.


----------


.. eql:function:: ext::postgis::convexhull( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_convexhull``.


----------


.. eql:function:: ext::postgis::coorddim( \
                    geometry: ext::postgis::geometry \
                  ) ->  std::int16

    This is exposing ``st_coorddim``.


----------


.. eql:function:: ext::postgis::coverageunion( \
                    a0: array<ext::postgis::geometry> \
                  ) ->  ext::postgis::geometry

    Computes polygonal coverage from a set of polygons.

    Computes the union of a set of polygons forming a coverage by removing
    shared edges.



    This is exposing ``st_coverageunion``.


----------


.. eql:function:: ext::postgis::coveredby( \
                    a0: optional std::str, \
                    a1: optional std::str, \
                  ) -> optional std::bool
                  ext::postgis::coveredby( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool
                  ext::postgis::coveredby( \
                    geog1: ext::postgis::geography, \
                    geog2: ext::postgis::geography, \
                  ) ->  std::bool

    This is exposing ``st_coveredby``.


----------


.. eql:function:: ext::postgis::covers( \
                    a0: optional std::str, \
                    a1: optional std::str, \
                  ) -> optional std::bool
                  ext::postgis::covers( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool
                  ext::postgis::covers( \
                    geog1: ext::postgis::geography, \
                    geog2: ext::postgis::geography, \
                  ) ->  std::bool

    This is exposing ``st_covers``.


----------


.. eql:function:: ext::postgis::cpawithin( \
                    a0: ext::postgis::geometry, \
                    a1: ext::postgis::geometry, \
                    a2: std::float64, \
                  ) ->  std::bool

    Tests if two trajectoriesis approach within the specified distance.

    Tests if the closest point of approach of two trajectoriesis within the
    specified distance.


    This is exposing ``st_cpawithin``.


----------


.. eql:function:: ext::postgis::crosses( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing ``st_crosses``.


----------


.. eql:function:: ext::postgis::curven( \
                    geometry: ext::postgis::geometry, \
                    i: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_curven``.


----------


.. eql:function:: ext::postgis::curvetoline( \
                    geom: ext::postgis::geometry, \
                    tol: std::float64 = 32, \
                    toltype: std::int64 = 0, \
                    flags: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_curvetoline``.


----------


.. eql:function:: ext::postgis::delaunaytriangles( \
                    g1: ext::postgis::geometry, \
                    tolerance: std::float64 = 0.0, \
                    flags: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_delaunaytriangles``.


----------


.. eql:function:: ext::postgis::dfullywithin( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64, \
                  ) ->  std::bool

    Tests if two geometries are entirely within a given distance.

    This is exposing ``st_dfullywithin``.


----------


.. eql:function:: ext::postgis::dfullywithin3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64, \
                  ) ->  std::bool

    This is exposing ``st_3ddfullywithin``.


----------


.. eql:function:: ext::postgis::difference( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    gridsize: std::float64 = -1.0, \
                  ) ->  ext::postgis::geometry

    Computes a geometry resulting from removing all points in *geom2* from
    *geom1*.

    Computes a geometry representing the part of geometry *geom1* that does
    not intersect geometry *geom2*.


    This is exposing ``st_difference``.


----------


.. eql:function:: ext::postgis::dimension( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_dimension``.


----------


.. eql:function:: ext::postgis::disjoint( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing ``st_disjoint``.


----------


.. eql:function:: ext::postgis::distance( \
                    a0: std::str, \
                    a1: std::str, \
                  ) ->  std::float64
                  ext::postgis::distance( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64
                  ext::postgis::distance( \
                    geog1: ext::postgis::geography, \
                    geog2: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  std::float64

    This is exposing ``st_distance``.


----------


.. eql:function:: ext::postgis::distance3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64

    Returns the 3D cartesian minimum distance between two geometries.

    Returns the 3D cartesian minimum distance (based on spatial ref) between
    two geometries in projected units.


    This is exposing ``st_3ddistance``.


----------


.. eql:function:: ext::postgis::distancecpa( \
                    a0: ext::postgis::geometry, \
                    a1: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing ``st_distancecpa``.


----------


.. eql:function:: ext::postgis::distancesphere( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64
                  ext::postgis::distancesphere( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    radius: std::float64, \
                  ) ->  std::float64

    This is exposing ``st_distancesphere``.


----------


.. eql:function:: ext::postgis::distancespheroid( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing ``st_distancespheroid``.


----------


.. eql:function:: ext::postgis::dwithin( \
                    a0: optional std::str, \
                    a1: optional std::str, \
                    a2: optional std::float64, \
                  ) -> optional std::bool
                  ext::postgis::dwithin( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64, \
                  ) ->  std::bool
                  ext::postgis::dwithin( \
                    geog1: ext::postgis::geography, \
                    geog2: ext::postgis::geography, \
                    tolerance: std::float64, \
                    use_spheroid: std::bool = true, \
                  ) ->  std::bool

    This is exposing ``st_dwithin``.


----------


.. eql:function:: ext::postgis::dwithin3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64, \
                  ) ->  std::bool

    This is exposing ``st_3ddwithin``.


----------


.. eql:function:: ext::postgis::endpoint( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_endpoint``.


----------


.. eql:function:: ext::postgis::envelope( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_envelope``.


----------


.. eql:function:: ext::postgis::equals( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing ``st_equals``.


----------


.. eql:function:: ext::postgis::expand( \
                    a0: ext::postgis::box2d, \
                    a1: std::float64, \
                  ) ->  ext::postgis::box2d
                  ext::postgis::expand( \
                    a0: ext::postgis::box3d, \
                    a1: std::float64, \
                  ) ->  ext::postgis::box3d
                  ext::postgis::expand( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::expand( \
                    box: ext::postgis::box2d, \
                    dx: std::float64, \
                    dy: std::float64, \
                  ) ->  ext::postgis::box2d
                  ext::postgis::expand( \
                    box: ext::postgis::box3d, \
                    dx: std::float64, \
                    dy: std::float64, \
                    dz: std::float64 = 0, \
                  ) ->  ext::postgis::box3d
                  ext::postgis::expand( \
                    geom: ext::postgis::geometry, \
                    dx: std::float64, \
                    dy: std::float64, \
                    dz: std::float64 = 0, \
                    dm: std::float64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_expand``.


----------


.. eql:function:: ext::postgis::exteriorring( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_exteriorring``.


----------


.. eql:function:: ext::postgis::filterbym( \
                    a0: optional ext::postgis::geometry, \
                    a1: optional std::float64, \
                    a2: optional std::float64 = {}, \
                    a3: optional std::bool = false, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_filterbym``.


----------


.. eql:function:: ext::postgis::flipcoordinates( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_flipcoordinates``.


----------


.. eql:function:: ext::postgis::force2d( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_force2d``.


----------


.. eql:function:: ext::postgis::force3d( \
                    geom: ext::postgis::geometry, \
                    zvalue: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_force3d``.


----------


.. eql:function:: ext::postgis::force3dm( \
                    geom: ext::postgis::geometry, \
                    mvalue: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_force3dm``.


----------


.. eql:function:: ext::postgis::force3dz( \
                    geom: ext::postgis::geometry, \
                    zvalue: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_force3dz``.


----------


.. eql:function:: ext::postgis::force4d( \
                    geom: ext::postgis::geometry, \
                    zvalue: std::float64 = 0.0, \
                    mvalue: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_force4d``.


----------


.. eql:function:: ext::postgis::forcecollection( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_forcecollection``.


----------


.. eql:function:: ext::postgis::forcecurve( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_forcecurve``.


----------


.. eql:function:: ext::postgis::forcepolygonccw( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_forcepolygonccw``.


----------


.. eql:function:: ext::postgis::forcepolygoncw( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_forcepolygoncw``.


----------


.. eql:function:: ext::postgis::forcerhr( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    Forces the orientation of the vertices in a polygon to follow the RHR.

    Forces the orientation of the vertices in a polygon to follow a
    Right-Hand-Rule, in which the area that is bounded by the polygon is to
    the right of the boundary. In particular, the exterior ring is orientated
    in a clockwise direction and the interior rings in a counter-clockwise
    direction.


    This is exposing ``st_forcerhr``.


----------


.. eql:function:: ext::postgis::forcesfs( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry
                  ext::postgis::forcesfs( \
                    a0: ext::postgis::geometry, \
                    version: std::str, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_forcesfs``.


----------


.. eql:function:: ext::postgis::frechetdistance( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64 = -1, \
                  ) ->  std::float64

    This is exposing ``st_frechetdistance``.


----------


.. eql:function:: ext::postgis::generatepoints( \
                    area: ext::postgis::geometry, \
                    npoints: std::int64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::generatepoints( \
                    area: ext::postgis::geometry, \
                    npoints: std::int64, \
                    seed: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_generatepoints``.


----------


.. eql:function:: ext::postgis::geogfromtext( \
                    a0: std::str \
                  ) ->  ext::postgis::geography

    Creates a geography value from WKT or EWTK.

    Creates a geography value from Well-Known Text or Extended
    Well-Known Text representation.


    This is exposing ``st_geogfromtext``.


----------


.. eql:function:: ext::postgis::geogfromwkb( \
                    a0: std::bytes \
                  ) ->  ext::postgis::geography

    Creates a geography value from WKB or EWKB.

    Creates a geography value from a Well-Known Binary geometry representation
    (WKB) or extended Well Known Binary (EWKB).


    This is exposing ``st_geogfromwkb``.


----------


.. eql:function:: ext::postgis::geography_cmp( \
                    a0: ext::postgis::geography, \
                    a1: ext::postgis::geography, \
                  ) ->  std::int64

    This is exposing ``geography_cmp``.


----------


.. eql:function:: ext::postgis::geohash( \
                    geom: ext::postgis::geometry, \
                    maxchars: std::int64 = 0, \
                  ) ->  std::str
                  ext::postgis::geohash( \
                    geog: ext::postgis::geography, \
                    maxchars: std::int64 = 0, \
                  ) ->  std::str

    This is exposing ``st_geohash``.


----------


.. eql:function:: ext::postgis::geomcollfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::geomcollfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    Makes a collection Geometry from collection WKT.

    Makes a collection Geometry from collection WKT with the given SRID. If
    SRID is not given, it defaults to 0.


    This is exposing ``st_geomcollfromtext``.


----------


.. eql:function:: ext::postgis::geomcollfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::geomcollfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_geomcollfromwkb``.


----------


.. eql:function:: ext::postgis::geometricmedian( \
                    g: optional ext::postgis::geometry, \
                    tolerance: optional std::float64 = {}, \
                    max_iter: optional std::int64 = 10000, \
                    fail_if_not_converged: optional std::bool = false, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_geometricmedian``.


----------


.. eql:function:: ext::postgis::geometry_cmp( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::int64

    This is exposing ``geometry_cmp``.


----------


.. eql:function:: ext::postgis::geometry_hash( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``geometry_hash``.


----------


.. eql:function:: ext::postgis::geometryn( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_geometryn``.


----------


.. eql:function:: ext::postgis::geometrytype( \
                    a0: ext::postgis::geometry \
                  ) ->  std::str
                  ext::postgis::geometrytype( \
                    a0: ext::postgis::geography \
                  ) ->  std::str

    This is exposing ``geometrytype``.


----------


.. eql:function:: ext::postgis::geomfromewkb( \
                    a0: std::bytes \
                  ) ->  ext::postgis::geometry

    Creates a geometry value from EWKB.

    Creates a geometry value from Extended Well-Known Binary representation
    (EWKB).


    This is exposing ``st_geomfromewkb``.


----------


.. eql:function:: ext::postgis::geomfromewkt( \
                    a0: std::str \
                  ) ->  ext::postgis::geometry

    Creates a geometry value from EWKT representation.

    Creates a geometry value from Extended Well-Known Text representation (EWKT).


    This is exposing ``st_geomfromewkt``.


----------


.. eql:function:: ext::postgis::geomfromgeohash( \
                    a0: optional std::str, \
                    a1: optional std::int64 = {}, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_geomfromgeohash``.


----------


.. eql:function:: ext::postgis::geomfromgeojson( \
                    a0: std::str \
                  ) ->  ext::postgis::geometry
                  ext::postgis::geomfromgeojson( \
                    a0: std::json \
                  ) ->  ext::postgis::geometry

    Creates a geometry value from a geojson representation of a geometry.

    Takes as input a geojson representation of a geometry and outputs a
    ``geometry`` value.


    This is exposing ``st_geomfromgeojson``.


----------


.. eql:function:: ext::postgis::geomfromgml( \
                    a0: std::str \
                  ) ->  ext::postgis::geometry
                  ext::postgis::geomfromgml( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    Creates a geometry value from GML representation of a geometry.

    Takes as input GML representation of geometry and outputs a  ``geometry``
    value.


    This is exposing ``st_geomfromgml``.


----------


.. eql:function:: ext::postgis::geomfromkml( \
                    a0: std::str \
                  ) ->  ext::postgis::geometry

    Creates a geometry value from KML representation of a geometry.

    Takes as input KML representation of geometry and outputs a ``geometry``
    value.


    This is exposing ``st_geomfromkml``.


----------


.. eql:function:: ext::postgis::geomfrommarc21( \
                    marc21xml: std::str \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_geomfrommarc21``.


----------


.. eql:function:: ext::postgis::geomfromtext( \
                    a0: std::str \
                  ) ->  ext::postgis::geometry
                  ext::postgis::geomfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    Creates a geometry value from WKT representation.

    Creates a geometry value from Well-Known Text representation (WKT).


    This is exposing ``st_geomfromtext``.


----------


.. eql:function:: ext::postgis::geomfromtwkb( \
                    a0: std::bytes \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_geomfromtwkb``.


----------


.. eql:function:: ext::postgis::geomfromwkb( \
                    a0: std::bytes \
                  ) ->  ext::postgis::geometry
                  ext::postgis::geomfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    Creates a geometry value from WKB representation.

    Creates a geometry value from a Well-Known Binary geometry representation
    (WKB) and optional SRID.


    This is exposing ``st_geomfromwkb``.


----------


.. eql:function:: ext::postgis::get_proj4_from_srid( \
                    a0: std::int64 \
                  ) ->  std::str

    This is exposing ``get_proj4_from_srid``.


----------


.. eql:function:: ext::postgis::hasarc( \
                    geometry: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_hasarc``.


----------


.. eql:function:: ext::postgis::hasm( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_hasm``.


----------


.. eql:function:: ext::postgis::hasz( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_hasz``.


----------


.. eql:function:: ext::postgis::hausdorffdistance( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64
                  ext::postgis::hausdorffdistance( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64, \
                  ) ->  std::float64

    This is exposing ``st_hausdorffdistance``.


----------


.. eql:function:: ext::postgis::hexagon( \
                    size: std::float64, \
                    cell_i: std::int64, \
                    cell_j: std::int64, \
                    origin: ext::postgis::geometry = 'POINT(0 0)', \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_hexagon``.


----------


.. eql:function:: ext::postgis::interiorringn( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_interiorringn``.


----------


.. eql:function:: ext::postgis::interpolatepoint( \
                    line: ext::postgis::geometry, \
                    point: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing ``st_interpolatepoint``.


----------


.. eql:function:: ext::postgis::intersection( \
                    a0: std::str, \
                    a1: std::str, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::intersection( \
                    a0: ext::postgis::geography, \
                    a1: ext::postgis::geography, \
                  ) ->  ext::postgis::geography
                  ext::postgis::intersection( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    gridsize: std::float64 = -1, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_intersection``.


----------


.. eql:function:: ext::postgis::intersects( \
                    a0: optional std::str, \
                    a1: optional std::str, \
                  ) -> optional std::bool
                  ext::postgis::intersects( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool
                  ext::postgis::intersects( \
                    geog1: ext::postgis::geography, \
                    geog2: ext::postgis::geography, \
                  ) ->  std::bool

    This is exposing ``st_intersects``.


----------


.. eql:function:: ext::postgis::intersects3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    Tests if two geometries spatially intersect in 3D.

    Tests if two geometries spatially intersect in 3D - only for points,
    linestrings, polygons, polyhedral surface (area).


    This is exposing ``st_3dintersects``.


----------


.. eql:function:: ext::postgis::inversetransformpipeline( \
                    geom: ext::postgis::geometry, \
                    pipeline: std::str, \
                    to_srid: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_inversetransformpipeline``.


----------


.. eql:function:: ext::postgis::isclosed( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    Tests if a geometry in 2D or 3D is closed.

    Tests if a LineStrings's start and end points are coincident. For a
    PolyhedralSurface tests if it is closed (volumetric).


    This is exposing ``st_isclosed``.


----------


.. eql:function:: ext::postgis::iscollection( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_iscollection``.


----------


.. eql:function:: ext::postgis::isempty( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_isempty``.


----------


.. eql:function:: ext::postgis::ispolygonccw( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    Tests counter-clockwise poligonal orientation of a geometry.

    Tests if Polygons have exterior rings oriented counter-clockwise and
    interior rings oriented clockwise.


    This is exposing ``st_ispolygonccw``.


----------


.. eql:function:: ext::postgis::ispolygoncw( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    Tests clockwise poligonal orientation of a geometry.

    Tests if Polygons have exterior rings oriented clockwise and interior
    rings oriented counter-clockwise.


    This is exposing ``st_ispolygoncw``.


----------


.. eql:function:: ext::postgis::isring( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_isring``.


----------


.. eql:function:: ext::postgis::issimple( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_issimple``.


----------


.. eql:function:: ext::postgis::isvalid( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool
                  ext::postgis::isvalid( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  std::bool

    This is exposing ``st_isvalid``.


----------


.. eql:function:: ext::postgis::isvalidreason( \
                    a0: ext::postgis::geometry \
                  ) ->  std::str
                  ext::postgis::isvalidreason( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  std::str

    This is exposing ``st_isvalidreason``.


----------


.. eql:function:: ext::postgis::isvalidtrajectory( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``st_isvalidtrajectory``.


----------


.. eql:function:: ext::postgis::length( \
                    a0: std::str \
                  ) ->  std::float64
                  ext::postgis::length( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64
                  ext::postgis::length( \
                    geog: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  std::float64

    This is exposing ``st_length``.


----------


.. eql:function:: ext::postgis::length2d( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_length2d``.


----------


.. eql:function:: ext::postgis::length3d( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_3dlength``.


----------


.. eql:function:: ext::postgis::linecrossingdirection( \
                    line1: ext::postgis::geometry, \
                    line2: ext::postgis::geometry, \
                  ) ->  std::int64

    This is exposing ``st_linecrossingdirection``.


----------


.. eql:function:: ext::postgis::lineextend( \
                    geom: ext::postgis::geometry, \
                    distance_forward: std::float64, \
                    distance_backward: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_lineextend``.


----------


.. eql:function:: ext::postgis::linefromencodedpolyline( \
                    txtin: std::str, \
                    nprecision: std::int64 = 5, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_linefromencodedpolyline``.


----------


.. eql:function:: ext::postgis::linefrommultipoint( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_linefrommultipoint``.


----------


.. eql:function:: ext::postgis::linefromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::linefromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    Creates a geometry from WKT LINESTRING.

    Makes a Geometry from WKT representation with the given SRID. If SRID is
    not given, it defaults to 0.


    This is exposing ``st_linefromtext``.


----------


.. eql:function:: ext::postgis::linefromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::linefromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_linefromwkb``.


----------


.. eql:function:: ext::postgis::lineinterpolatepoint( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::lineinterpolatepoint( \
                    a0: optional std::str, \
                    a1: optional std::float64, \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::lineinterpolatepoint( \
                    a0: ext::postgis::geography, \
                    a1: std::float64, \
                    use_spheroid: std::bool = true, \
                  ) ->  ext::postgis::geography

    This is exposing ``st_lineinterpolatepoint``.


----------


.. eql:function:: ext::postgis::lineinterpolatepoint3d( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_3dlineinterpolatepoint``.


----------


.. eql:function:: ext::postgis::lineinterpolatepoints( \
                    a0: optional std::str, \
                    a1: optional std::float64, \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::lineinterpolatepoints( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    repeat: std::bool = true, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::lineinterpolatepoints( \
                    a0: ext::postgis::geography, \
                    a1: std::float64, \
                    use_spheroid: std::bool = true, \
                    repeat: std::bool = true, \
                  ) ->  ext::postgis::geography

    This is exposing ``st_lineinterpolatepoints``.


----------


.. eql:function:: ext::postgis::linelocatepoint( \
                    a0: optional std::str, \
                    a1: optional std::str, \
                  ) -> optional std::float64
                  ext::postgis::linelocatepoint( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64
                  ext::postgis::linelocatepoint( \
                    a0: ext::postgis::geography, \
                    a1: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  std::float64

    This is exposing ``st_linelocatepoint``.


----------


.. eql:function:: ext::postgis::linemerge( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry
                  ext::postgis::linemerge( \
                    a0: ext::postgis::geometry, \
                    a1: std::bool, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_linemerge``.


----------


.. eql:function:: ext::postgis::linestringfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::linestringfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_linestringfromwkb``.


----------


.. eql:function:: ext::postgis::linesubstring( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::linesubstring( \
                    a0: ext::postgis::geography, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geography
                  ext::postgis::linesubstring( \
                    a0: optional std::str, \
                    a1: optional std::float64, \
                    a2: optional std::float64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_linesubstring``.


----------


.. eql:function:: ext::postgis::linetocurve( \
                    geometry: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_linetocurve``.


----------


.. eql:function:: ext::postgis::locatealong( \
                    geometry: ext::postgis::geometry, \
                    measure: std::float64, \
                    leftrightoffset: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_locatealong``.


----------


.. eql:function:: ext::postgis::locatebetween( \
                    geometry: ext::postgis::geometry, \
                    frommeasure: std::float64, \
                    tomeasure: std::float64, \
                    leftrightoffset: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_locatebetween``.


----------


.. eql:function:: ext::postgis::locatebetweenelevations( \
                    geometry: ext::postgis::geometry, \
                    fromelevation: std::float64, \
                    toelevation: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_locatebetweenelevations``.


----------


.. eql:function:: ext::postgis::longestline( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_longestline``.


----------


.. eql:function:: ext::postgis::longestline3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_3dlongestline``.


----------


.. eql:function:: ext::postgis::m( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_m``.


----------


.. eql:function:: ext::postgis::makebox2d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::box2d

    This is exposing ``st_makebox2d``.


----------


.. eql:function:: ext::postgis::makebox3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::box3d

    This is exposing ``st_3dmakebox``.


----------


.. eql:function:: ext::postgis::makeenvelope( \
                    a0: std::float64, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                    a4: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_makeenvelope``.


----------


.. eql:function:: ext::postgis::makeline( \
                    a0: array<ext::postgis::geometry> \
                  ) ->  ext::postgis::geometry
                  ext::postgis::makeline( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_makeline``.


----------


.. eql:function:: ext::postgis::makepoint( \
                    a0: std::float64, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::makepoint( \
                    a0: std::float64, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::makepoint( \
                    a0: std::float64, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_makepoint``.


----------


.. eql:function:: ext::postgis::makepointm( \
                    a0: std::float64, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_makepointm``.


----------


.. eql:function:: ext::postgis::makepolygon( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry
                  ext::postgis::makepolygon( \
                    a0: ext::postgis::geometry, \
                    a1: array<ext::postgis::geometry>, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_makepolygon``.


----------


.. eql:function:: ext::postgis::makevalid( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry
                  ext::postgis::makevalid( \
                    geom: ext::postgis::geometry, \
                    params: std::str, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_makevalid``.


----------


.. eql:function:: ext::postgis::maxdistance( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64

    This is exposing ``st_maxdistance``.


----------


.. eql:function:: ext::postgis::maxdistance3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::float64

    Returns the 3D cartesian maximum distance between two geometries.

    Returns the 3D cartesian maximum distance (based on spatial ref) between
    two geometries in projected units.


    This is exposing ``st_3dmaxdistance``.


----------


.. eql:function:: ext::postgis::memsize( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_memsize``.


----------


.. eql:function:: ext::postgis::minimumboundingcircle( \
                    inputgeom: ext::postgis::geometry, \
                    segs_per_quarter: std::int64 = 48, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_minimumboundingcircle``.


----------


.. eql:function:: ext::postgis::minimumclearance( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_minimumclearance``.


----------


.. eql:function:: ext::postgis::minimumclearanceline( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_minimumclearanceline``.


----------


.. eql:function:: ext::postgis::mlinefromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::mlinefromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_mlinefromtext``.


----------


.. eql:function:: ext::postgis::mlinefromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::mlinefromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_mlinefromwkb``.


----------


.. eql:function:: ext::postgis::mpointfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::mpointfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    Creates a geometry from WKT MULTIPOINT.

    Makes a Geometry from WKT with the given SRID. If SRID is not given, it
    defaults to 0.


    This is exposing ``st_mpointfromtext``.


----------


.. eql:function:: ext::postgis::mpointfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::mpointfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_mpointfromwkb``.


----------


.. eql:function:: ext::postgis::mpolyfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::mpolyfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    Creates a geometry from WKT MULTIPOLYGON.

    Makes a MultiPolygon Geometry from WKT with the given SRID. If SRID is not
    given, it defaults to 0.


    This is exposing ``st_mpolyfromtext``.


----------


.. eql:function:: ext::postgis::mpolyfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::mpolyfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_mpolyfromwkb``.


----------


.. eql:function:: ext::postgis::multi( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_multi``.


----------


.. eql:function:: ext::postgis::multilinefromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_multilinefromwkb``.


----------


.. eql:function:: ext::postgis::multilinestringfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::multilinestringfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_multilinestringfromtext``.


----------


.. eql:function:: ext::postgis::multipointfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_multipointfromtext``.


----------


.. eql:function:: ext::postgis::multipointfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::multipointfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_multipointfromwkb``.


----------


.. eql:function:: ext::postgis::multipolyfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::multipolyfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_multipolyfromwkb``.


----------


.. eql:function:: ext::postgis::multipolygonfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::multipolygonfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_multipolygonfromtext``.


----------


.. eql:function:: ext::postgis::ndims( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int16

    This is exposing ``st_ndims``.


----------


.. eql:function:: ext::postgis::node( \
                    g: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_node``.


----------


.. eql:function:: ext::postgis::normalize( \
                    geom: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_normalize``.


----------


.. eql:function:: ext::postgis::npoints( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_npoints``.


----------


.. eql:function:: ext::postgis::nrings( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_nrings``.


----------


.. eql:function:: ext::postgis::numcurves( \
                    geometry: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_numcurves``.


----------


.. eql:function:: ext::postgis::numgeometries( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_numgeometries``.


----------


.. eql:function:: ext::postgis::numinteriorring( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_numinteriorring``.


----------


.. eql:function:: ext::postgis::numinteriorrings( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_numinteriorrings``.


----------


.. eql:function:: ext::postgis::numpatches( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    Return the number of faces on a Polyhedral Surface.

    This is exposing ``st_numpatches``.


----------


.. eql:function:: ext::postgis::numpoints( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int64

    This is exposing ``st_numpoints``.


----------


.. eql:function:: ext::postgis::offsetcurve( \
                    line: ext::postgis::geometry, \
                    distance: std::float64, \
                    params: std::str = '', \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_offsetcurve``.


----------


.. eql:function:: ext::postgis::orderingequals( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    Tests if two geometries are the same geometry including points order.

    Tests if two geometries represent the same geometry and have points in the same directional order.


    This is exposing ``st_orderingequals``.


----------


.. eql:function:: ext::postgis::orientedenvelope( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_orientedenvelope``.


----------


.. eql:function:: ext::postgis::overlaps( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    Tests if two geometries overlap.

    Tests if two geometries have the same dimension and intersect, but each
    has at least one point not in the other.


    This is exposing ``st_overlaps``.


----------


.. eql:function:: ext::postgis::patchn( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_patchn``.


----------


.. eql:function:: ext::postgis::perimeter( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64
                  ext::postgis::perimeter( \
                    geog: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  std::float64

    This is exposing ``st_perimeter``.


----------


.. eql:function:: ext::postgis::perimeter2d( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_perimeter2d``.


----------


.. eql:function:: ext::postgis::perimeter3d( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_3dperimeter``.


----------


.. eql:function:: ext::postgis::point( \
                    a0: std::float64, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::point( \
                    a0: std::float64, \
                    a1: std::float64, \
                    srid: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_point``.


----------


.. eql:function:: ext::postgis::pointfromgeohash( \
                    a0: optional std::str, \
                    a1: optional std::int64 = {}, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_pointfromgeohash``.


----------


.. eql:function:: ext::postgis::pointfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::pointfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    Makes a POINT geometry from WKT.

    Makes a POINT geometry from WKT with the given SRID. If SRID is not given,
    it defaults to unknown.


    This is exposing ``st_pointfromtext``.


----------


.. eql:function:: ext::postgis::pointfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::pointfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_pointfromwkb``.


----------


.. eql:function:: ext::postgis::pointinsidecircle( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                  ) ->  std::bool

    This is exposing ``st_pointinsidecircle``.


----------


.. eql:function:: ext::postgis::pointm( \
                    xcoordinate: std::float64, \
                    ycoordinate: std::float64, \
                    mcoordinate: std::float64, \
                    srid: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_pointm``.


----------


.. eql:function:: ext::postgis::pointn( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    Returns the Nth point in the first LineString in a geometry.

    Returns the Nth point in the first LineString or circular LineString in a
    geometry.


    This is exposing ``st_pointn``.


----------


.. eql:function:: ext::postgis::pointonsurface( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_pointonsurface``.


----------


.. eql:function:: ext::postgis::points( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_points``.


----------


.. eql:function:: ext::postgis::pointz( \
                    xcoordinate: std::float64, \
                    ycoordinate: std::float64, \
                    zcoordinate: std::float64, \
                    srid: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_pointz``.


----------


.. eql:function:: ext::postgis::pointzm( \
                    xcoordinate: std::float64, \
                    ycoordinate: std::float64, \
                    zcoordinate: std::float64, \
                    mcoordinate: std::float64, \
                    srid: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_pointzm``.


----------


.. eql:function:: ext::postgis::polyfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::polyfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_polyfromtext``.


----------


.. eql:function:: ext::postgis::polyfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::polyfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_polyfromwkb``.


----------


.. eql:function:: ext::postgis::polygon( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_polygon``.


----------


.. eql:function:: ext::postgis::polygonfromtext( \
                    a0: std::str \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::polygonfromtext( \
                    a0: std::str, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    Creates a geometry from WKT POLYGON.

    Makes a Geometry from WKT with the given SRID. If SRID is not given, it defaults to 0.


    This is exposing ``st_polygonfromtext``.


----------


.. eql:function:: ext::postgis::polygonfromwkb( \
                    a0: std::bytes \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::polygonfromwkb( \
                    a0: std::bytes, \
                    a1: std::int64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_polygonfromwkb``.


----------


.. eql:function:: ext::postgis::polygonize( \
                    a0: array<ext::postgis::geometry> \
                  ) ->  ext::postgis::geometry

    Computes a collection of polygons formed from a set of linework.

    Computes a collection of polygons formed from the linework of a set of
    geometries.


    This is exposing ``st_polygonize``.


----------


.. eql:function:: ext::postgis::postgis_addbbox( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``postgis_addbbox``.


----------


.. eql:function:: ext::postgis::postgis_constraint_dims( \
                    geomschema: std::str, \
                    geomtable: std::str, \
                    geomcolumn: std::str, \
                  ) ->  std::int64

    This is exposing ``postgis_constraint_dims``.


----------


.. eql:function:: ext::postgis::postgis_constraint_srid( \
                    geomschema: std::str, \
                    geomtable: std::str, \
                    geomcolumn: std::str, \
                  ) ->  std::int64

    This is exposing ``postgis_constraint_srid``.


----------


.. eql:function:: ext::postgis::postgis_dropbbox( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``postgis_dropbbox``.


----------


.. eql:function:: ext::postgis::postgis_full_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_full_version``.


----------


.. eql:function:: ext::postgis::postgis_geos_compiled_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_geos_compiled_version``.


----------


.. eql:function:: ext::postgis::postgis_geos_noop( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``postgis_geos_noop``.


----------


.. eql:function:: ext::postgis::postgis_geos_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_geos_version``.


----------


.. eql:function:: ext::postgis::postgis_getbbox( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::box2d

    This is exposing ``postgis_getbbox``.


----------


.. eql:function:: ext::postgis::postgis_hasbbox( \
                    a0: ext::postgis::geometry \
                  ) ->  std::bool

    This is exposing ``postgis_hasbbox``.


----------


.. eql:function:: ext::postgis::postgis_lib_build_date( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_lib_build_date``.


----------


.. eql:function:: ext::postgis::postgis_lib_revision( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_lib_revision``.


----------


.. eql:function:: ext::postgis::postgis_lib_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_lib_version``.


----------


.. eql:function:: ext::postgis::postgis_libjson_version( \
                     \
                  ) ->  std::str

    This is exposing ``postgis_libjson_version``.


----------


.. eql:function:: ext::postgis::postgis_liblwgeom_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_liblwgeom_version``.


----------


.. eql:function:: ext::postgis::postgis_libprotobuf_version( \
                     \
                  ) ->  std::str

    This is exposing ``postgis_libprotobuf_version``.


----------


.. eql:function:: ext::postgis::postgis_libxml_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_libxml_version``.


----------


.. eql:function:: ext::postgis::postgis_noop( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``postgis_noop``.


----------


.. eql:function:: ext::postgis::postgis_proj_compiled_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_proj_compiled_version``.


----------


.. eql:function:: ext::postgis::postgis_proj_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_proj_version``.


----------


.. eql:function:: ext::postgis::postgis_scripts_build_date( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_scripts_build_date``.


----------


.. eql:function:: ext::postgis::postgis_scripts_installed( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_scripts_installed``.


----------


.. eql:function:: ext::postgis::postgis_scripts_released( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_scripts_released``.


----------


.. eql:function:: ext::postgis::postgis_srs_codes( \
                    auth_name: std::str \
                  ) ->  std::str

    This is exposing ``postgis_srs_codes``.


----------


.. eql:function:: ext::postgis::postgis_svn_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_svn_version``.


----------


.. eql:function:: ext::postgis::postgis_transform_geometry( \
                    geom: ext::postgis::geometry, \
                    a1: std::str, \
                    a2: std::str, \
                    a3: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``postgis_transform_geometry``.


----------


.. eql:function:: ext::postgis::postgis_transform_pipeline_geometry( \
                    geom: ext::postgis::geometry, \
                    pipeline: std::str, \
                    forward: std::bool, \
                    to_srid: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``postgis_transform_pipeline_geometry``.


----------


.. eql:function:: ext::postgis::postgis_typmod_dims( \
                    a0: std::int64 \
                  ) ->  std::int64

    This is exposing ``postgis_typmod_dims``.


----------


.. eql:function:: ext::postgis::postgis_typmod_srid( \
                    a0: std::int64 \
                  ) ->  std::int64

    This is exposing ``postgis_typmod_srid``.


----------


.. eql:function:: ext::postgis::postgis_typmod_type( \
                    a0: std::int64 \
                  ) ->  std::str

    This is exposing ``postgis_typmod_type``.


----------


.. eql:function:: ext::postgis::postgis_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_version``.


----------


.. eql:function:: ext::postgis::postgis_wagyu_version( \
                     \
                  ) -> optional std::str

    This is exposing ``postgis_wagyu_version``.


----------


.. eql:function:: ext::postgis::project( \
                    geom1: ext::postgis::geometry, \
                    distance: std::float64, \
                    azimuth: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::project( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    distance: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::project( \
                    geog_from: ext::postgis::geography, \
                    geog_to: ext::postgis::geography, \
                    distance: std::float64, \
                  ) ->  ext::postgis::geography
                  ext::postgis::project( \
                    geog: optional ext::postgis::geography, \
                    distance: optional std::float64, \
                    azimuth: optional std::float64, \
                  ) -> optional ext::postgis::geography

    Returns a point projected from a start point by a distance and bearing.

    This is exposing ``st_project``.


----------


.. eql:function:: ext::postgis::quantizecoordinates( \
                    g: optional ext::postgis::geometry, \
                    prec_x: optional std::int64, \
                    prec_y: optional std::int64 = {}, \
                    prec_z: optional std::int64 = {}, \
                    prec_m: optional std::int64 = {}, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_quantizecoordinates``.


----------


.. eql:function:: ext::postgis::reduceprecision( \
                    geom: ext::postgis::geometry, \
                    gridsize: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_reduceprecision``.


----------


.. eql:function:: ext::postgis::relate( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::str
                  ext::postgis::relate( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::str, \
                  ) ->  std::bool
                  ext::postgis::relate( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::int64, \
                  ) ->  std::str

    Tests if two geometries have a topological relationship.

    Tests if two geometries have a topological relationship matching an
    Intersection Matrix pattern, or computes their Intersection Matrix.


    This is exposing ``st_relate``.


----------


.. eql:function:: ext::postgis::relatematch( \
                    a0: std::str, \
                    a1: std::str, \
                  ) ->  std::bool

    This is exposing ``st_relatematch``.


----------


.. eql:function:: ext::postgis::removeirrelevantpointsforview( \
                    a0: ext::postgis::geometry, \
                    a1: ext::postgis::box2d, \
                    a2: std::bool = false, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_removeirrelevantpointsforview``.


----------


.. eql:function:: ext::postgis::removepoint( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_removepoint``.


----------


.. eql:function:: ext::postgis::removerepeatedpoints( \
                    geom: ext::postgis::geometry, \
                    tolerance: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_removerepeatedpoints``.


----------


.. eql:function:: ext::postgis::removesmallparts( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_removesmallparts``.


----------


.. eql:function:: ext::postgis::reverse( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_reverse``.


----------


.. eql:function:: ext::postgis::rotate( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::rotate( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::rotate( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_rotate``.


----------


.. eql:function:: ext::postgis::rotatex( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_rotatex``.


----------


.. eql:function:: ext::postgis::rotatey( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_rotatey``.


----------


.. eql:function:: ext::postgis::rotatez( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_rotatez``.


----------


.. eql:function:: ext::postgis::scale( \
                    a0: ext::postgis::geometry, \
                    a1: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::scale( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::scale( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::scale( \
                    a0: ext::postgis::geometry, \
                    a1: ext::postgis::geometry, \
                    origin: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_scale``.


----------


.. eql:function:: ext::postgis::scroll( \
                    a0: ext::postgis::geometry, \
                    a1: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_scroll``.


----------


.. eql:function:: ext::postgis::segmentize( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::segmentize( \
                    geog: ext::postgis::geography, \
                    max_segment_length: std::float64, \
                  ) ->  ext::postgis::geography

    Makes a new geometry/geography with no segment longer than a given
    distance.


    This is exposing ``st_segmentize``.


----------


.. eql:function:: ext::postgis::seteffectivearea( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64 = -1, \
                    a2: std::int64 = 1, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_seteffectivearea``.


----------


.. eql:function:: ext::postgis::setpoint( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                    a2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_setpoint``.


----------


.. eql:function:: ext::postgis::setsrid( \
                    geom: ext::postgis::geometry, \
                    srid: std::int64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::setsrid( \
                    geog: ext::postgis::geography, \
                    srid: std::int64, \
                  ) ->  ext::postgis::geography

    This is exposing ``st_setsrid``.


----------


.. eql:function:: ext::postgis::sharedpaths( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_sharedpaths``.


----------


.. eql:function:: ext::postgis::shiftlongitude( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_shiftlongitude``.


----------


.. eql:function:: ext::postgis::shortestline( \
                    a0: optional std::str, \
                    a1: optional std::str, \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::shortestline( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::shortestline( \
                    a0: ext::postgis::geography, \
                    a1: ext::postgis::geography, \
                    use_spheroid: std::bool = true, \
                  ) ->  ext::postgis::geography

    This is exposing ``st_shortestline``.


----------


.. eql:function:: ext::postgis::shortestline3d( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_3dshortestline``.


----------


.. eql:function:: ext::postgis::simplify( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::simplify( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::bool, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_simplify``.


----------


.. eql:function:: ext::postgis::simplifypolygonhull( \
                    geom: ext::postgis::geometry, \
                    vertex_fraction: std::float64, \
                    is_outer: std::bool = true, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_simplifypolygonhull``.


----------


.. eql:function:: ext::postgis::simplifypreservetopology( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_simplifypreservetopology``.


----------


.. eql:function:: ext::postgis::simplifyvw( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_simplifyvw``.


----------


.. eql:function:: ext::postgis::snap( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_snap``.


----------


.. eql:function:: ext::postgis::snaptogrid( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::snaptogrid( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::snaptogrid( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                    a4: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::snaptogrid( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    a2: std::float64, \
                    a3: std::float64, \
                    a4: std::float64, \
                    a5: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_snaptogrid``.


----------


.. eql:function:: ext::postgis::split( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_split``.


----------


.. eql:function:: ext::postgis::square( \
                    size: std::float64, \
                    cell_i: std::int64, \
                    cell_j: std::int64, \
                    origin: ext::postgis::geometry = 'POINT(0 0)', \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_square``.


----------


.. eql:function:: ext::postgis::srid( \
                    geom: ext::postgis::geometry \
                  ) ->  std::int64
                  ext::postgis::srid( \
                    geog: ext::postgis::geography \
                  ) ->  std::int64

    This is exposing ``st_srid``.


----------


.. eql:function:: ext::postgis::startpoint( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_startpoint``.


----------


.. eql:function:: ext::postgis::subdivide( \
                    geom: ext::postgis::geometry, \
                    maxvertices: std::int64 = 256, \
                    gridsize: std::float64 = -1.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_subdivide``.


----------


.. eql:function:: ext::postgis::summary( \
                    a0: ext::postgis::geometry \
                  ) ->  std::str
                  ext::postgis::summary( \
                    a0: ext::postgis::geography \
                  ) ->  std::str

    This is exposing ``st_summary``.


----------


.. eql:function:: ext::postgis::symdifference( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    gridsize: std::float64 = -1.0, \
                  ) ->  ext::postgis::geometry

    Merges two geometries excluding where they intersect.

    This is exposing ``st_symdifference``.


----------


.. eql:function:: ext::postgis::symmetricdifference( \
                    geom1: optional ext::postgis::geometry, \
                    geom2: optional ext::postgis::geometry, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_symmetricdifference``.


----------


.. eql:function:: ext::postgis::tileenvelope( \
                    zoom: std::int64, \
                    x: std::int64, \
                    y: std::int64, \
                    bounds: ext::postgis::geometry = <ext::postgis::geometry>'SRID=3857;LINESTRING(-20037508.342789244 -20037508.342789244, \
                    20037508.342789244 20037508.342789244)', \
                    margin: std::float64 = 0.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_tileenvelope``.


----------


.. eql:function:: ext::postgis::to_box2d( \
                    a0: ext::postgis::box3d \
                  ) ->  ext::postgis::box2d
                  ext::postgis::to_box2d( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::box2d

    This is exposing ``box2d``.


----------


.. eql:function:: ext::postgis::to_box3d( \
                    a0: ext::postgis::box2d \
                  ) ->  ext::postgis::box3d
                  ext::postgis::to_box3d( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::box3d

    This is exposing ``box3d``.


----------


.. eql:function:: ext::postgis::to_geography( \
                    a0: std::bytes \
                  ) ->  ext::postgis::geography
                  ext::postgis::to_geography( \
                    a0: ext::postgis::geometry \
                  ) ->  ext::postgis::geography

    This is exposing ``geography``.


----------


.. eql:function:: ext::postgis::to_geometry( \
                    a0: std::str \
                  ) ->  ext::postgis::geometry
                  ext::postgis::to_geometry( \
                    a0: std::bytes \
                  ) ->  ext::postgis::geometry
                  ext::postgis::to_geometry( \
                    a0: ext::postgis::box2d \
                  ) ->  ext::postgis::geometry
                  ext::postgis::to_geometry( \
                    a0: ext::postgis::box3d \
                  ) ->  ext::postgis::geometry
                  ext::postgis::to_geometry( \
                    a0: ext::postgis::geography \
                  ) ->  ext::postgis::geometry

    This is exposing ``geometry``.


----------


.. eql:function:: ext::postgis::touches( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    Tests if two geometries touch without intersecting interiors.

    Tests if two geometries have at least one point in common, but their interiors do not intersect.


    This is exposing ``st_touches``.


----------


.. eql:function:: ext::postgis::transform( \
                    a0: ext::postgis::geometry, \
                    a1: std::int64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::transform( \
                    geom: ext::postgis::geometry, \
                    to_proj: std::str, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::transform( \
                    geom: ext::postgis::geometry, \
                    from_proj: std::str, \
                    to_proj: std::str, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::transform( \
                    geom: ext::postgis::geometry, \
                    from_proj: std::str, \
                    to_srid: std::int64, \
                  ) ->  ext::postgis::geometry

    Transforms a geometry to a different spatial reference system.

    Returns a new geometry with coordinates transformed to a different spatial reference system.


    This is exposing ``st_transform``.


----------


.. eql:function:: ext::postgis::transformpipeline( \
                    geom: ext::postgis::geometry, \
                    pipeline: std::str, \
                    to_srid: std::int64 = 0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_transformpipeline``.


----------


.. eql:function:: ext::postgis::translate( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::translate( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_translate``.


----------


.. eql:function:: ext::postgis::transscale( \
                    a0: ext::postgis::geometry, \
                    a1: std::float64, \
                    a2: std::float64, \
                    a3: std::float64, \
                    a4: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_transscale``.


----------


.. eql:function:: ext::postgis::triangulatepolygon( \
                    g1: ext::postgis::geometry \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_triangulatepolygon``.


----------


.. eql:function:: ext::postgis::unaryunion( \
                    a0: ext::postgis::geometry, \
                    gridsize: std::float64 = -1.0, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_unaryunion``.


----------


.. eql:function:: ext::postgis::union( \
                    a0: array<ext::postgis::geometry> \
                  ) ->  ext::postgis::geometry
                  ext::postgis::union( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  ext::postgis::geometry
                  ext::postgis::union( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                    gridsize: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_union``.


----------


.. eql:function:: ext::postgis::voronoilines( \
                    g1: optional ext::postgis::geometry, \
                    tolerance: optional std::float64 = 0.0, \
                    extend_to: optional ext::postgis::geometry = {}, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_voronoilines``.


----------


.. eql:function:: ext::postgis::voronoipolygons( \
                    g1: optional ext::postgis::geometry, \
                    tolerance: optional std::float64 = 0.0, \
                    extend_to: optional ext::postgis::geometry = {}, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_voronoipolygons``.


----------


.. eql:function:: ext::postgis::within( \
                    geom1: ext::postgis::geometry, \
                    geom2: ext::postgis::geometry, \
                  ) ->  std::bool

    This is exposing ``st_within``.


----------


.. eql:function:: ext::postgis::wrapx( \
                    geom: ext::postgis::geometry, \
                    wrap: std::float64, \
                    `move`: std::float64, \
                  ) ->  ext::postgis::geometry

    This is exposing ``st_wrapx``.


----------


.. eql:function:: ext::postgis::x( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_x``.


----------


.. eql:function:: ext::postgis::xmax( \
                    a0: ext::postgis::box3d \
                  ) ->  std::float64

    This is exposing ``st_xmax``.


----------


.. eql:function:: ext::postgis::xmin( \
                    a0: ext::postgis::box3d \
                  ) ->  std::float64

    This is exposing ``st_xmin``.


----------


.. eql:function:: ext::postgis::y( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_y``.


----------


.. eql:function:: ext::postgis::ymax( \
                    a0: ext::postgis::box3d \
                  ) ->  std::float64

    This is exposing ``st_ymax``.


----------


.. eql:function:: ext::postgis::ymin( \
                    a0: ext::postgis::box3d \
                  ) ->  std::float64

    This is exposing ``st_ymin``.


----------


.. eql:function:: ext::postgis::z( \
                    a0: ext::postgis::geometry \
                  ) ->  std::float64

    This is exposing ``st_z``.


----------


.. eql:function:: ext::postgis::zmax( \
                    a0: ext::postgis::box3d \
                  ) ->  std::float64

    This is exposing ``st_zmax``.


----------


.. eql:function:: ext::postgis::zmflag( \
                    a0: ext::postgis::geometry \
                  ) ->  std::int16

    This is exposing ``st_zmflag``.


----------


.. eql:function:: ext::postgis::zmin( \
                    a0: ext::postgis::box3d \
                  ) ->  std::float64

    This is exposing ``st_zmin``.


Aggregates
==========

These functions operate of sets of geometric data.

----------


.. eql:function:: ext::postgis::clusterintersecting_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional array<ext::postgis::geometry>

    This is exposing ``st_clusterintersecting``.


----------


.. eql:function:: ext::postgis::clusterwithin_agg( \
                    a0: set of ext::postgis::geometry, \
                    a1: std::float64, \
                  ) -> optional array<ext::postgis::geometry>

    This is exposing ``st_clusterwithin``.


----------


.. eql:function:: ext::postgis::collect_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_collect``.


----------


.. eql:function:: ext::postgis::coverageunion_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::geometry

    Computes polygonal coverage from a set of polygons.

    Computes the union of a set of polygons forming a coverage by removing
    shared edges.



    This is exposing ``st_coverageunion``.


----------


.. eql:function:: ext::postgis::extent3d_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::box2d

    This is exposing ``st_3dextent``.


----------


.. eql:function:: ext::postgis::extent_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::box2d

    This is exposing ``st_extent``.


----------


.. eql:function:: ext::postgis::makeline_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_makeline``.


----------


.. eql:function:: ext::postgis::memunion_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::box2d

    This is exposing ``st_memunion``.


----------


.. eql:function:: ext::postgis::polygonize_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::geometry

    Computes a collection of polygons formed from a set of linework.

    Computes a collection of polygons formed from the linework of a set of
    geometries.


    This is exposing ``st_polygonize``.


----------


.. eql:function:: ext::postgis::union_agg( \
                    a0: set of ext::postgis::geometry \
                  ) -> optional ext::postgis::geometry
                  ext::postgis::union_agg( \
                    a0: set of ext::postgis::geometry, \
                    gridsize: std::float64, \
                  ) -> optional ext::postgis::geometry

    This is exposing ``st_union``.


.. _postgis:
    https://postgis.net/docs/manual-3.5/
