.. _ref_datamodel_modules_schema:

Schema
======


.. eql:type:: schema::Type

    :index: schema type introspect introspection

    Abstract base type for all the other types represented in the schema.

.. eql:type:: schema::ScalarType

    :index: schema scalar type introspect introspection

    The introspection information for any :ref:`scalar type
    <ref_datamodel_scalar_types>`.

    It can be queried directly in the ``schema`` module or via
    :eql:op:`INTROSPECT`.

.. eql:type:: schema::ObjectType

    :index: schema object type introspect introspection

    The introspection information for any :eql:type:`Object`.

    It can be queried directly in the ``schema`` module, via
    :eql:type:`__type__ <Object>` or via :eql:op:`INTROSPECT`.
