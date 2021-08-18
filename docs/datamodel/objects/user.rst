.. _ref_datamodel_object_types_user:

====================
User-defined Objects
====================

EdgeDB makes creating custom object types easy and intuitive. For
example, ``Person`` and ``Movie``:

.. code-block:: sdl

    type Person {
        required property name -> str {
            constraint exclusive;
        }
        multi link likes -> Movie;
    }
    type Movie {
        required property title -> str {
            constraint exclusive;
        }
    }

The ``Person`` type has a ``name`` :ref:`property
<ref_datamodel_props>` which takes on a :eql:type:`str` value. This
property is *required*, which means that is cannot be left unspecified
(or, equivalently, cannot take the value ``{}``). The ``name`` has the
:eql:constraint:`exclusive` :ref:`constraint
<ref_datamodel_constraints>`, which ensures that every ``Person`` has
a unique name. The ``Person`` type also has multiple :ref:`links
<ref_datamodel_links>` to ``Movie``. This means that multiple
``Movie`` objects can be associated with a ``Person`` by the link
``likes``. The ``Movie`` type just has a ``title`` property, which is
very similar to the ``name`` property of the ``Person``, since it's
also *required* and *exclusive*.

.. note::

    Since the empty string ``''`` is a *value*, required properties can
    take on ``''`` as their value.


See Also
--------

Object type
:ref:`SDL <ref_eql_sdl_object_types>`,
:ref:`DDL <ref_eql_ddl_object_types>`,
:ref:`introspection <ref_eql_introspection_object_types>`,
:ref:`links <ref_datamodel_links>`, and
:ref:`properties <ref_datamodel_props>`.
