.. _ref_eql_introspection:

Introspection
=============

All of the schema information in EdgeDB is stored in the ``schema``
:ref:`module <ref_datamodel_modules>` and is accessible via
*introspection queries*.

All the introspection types are themselves extending
:eql:type:`Object`, so they are also subject to introspection :ref:`as
object types <ref_eql_introspection_object_types>`. So the following
query will give a list of all the types used in introspection:

.. code-block:: edgeql

    SELECT name := schema::ObjectType.name
    FILTER name LIKE 'schema::%';

.. toctree::
    :maxdepth: 3
    :hidden:

    objects
    scalars
    colltypes
    functions
    indexes
    constraints
