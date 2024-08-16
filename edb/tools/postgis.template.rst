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

.. REFLECT: OPERATORS
Functions
=========

The core functions can be roughly grouped into the following categories.

.. REFLECT: CATEGORIES
.. REFLECT: FUNCTIONS
Aggregates
==========

These functions operate of sets of geometric data.

.. REFLECT: AGGREGATES
.. _postgis:
    https://postgis.net/docs/manual-3.4/
