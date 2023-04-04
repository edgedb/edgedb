.. _ref_datamodel_introspection:

Introspection
=============

All of the schema information in EdgeDB is stored in the ``schema``
:ref:`module <ref_datamodel_modules>` and is accessible via
*introspection queries*.

All the introspection types are themselves extending
:eql:type:`BaseObject`, so they are also subject to introspection :ref:`as
object types <ref_datamodel_introspection_object_types>`. The following
query will give a list of all the types used in introspection:

.. code-block:: edgeql

    select name := schema::ObjectType.name
    filter name like 'schema::%';

There's also a couple of ways of getting the introspection type of a
particular expression. Any :eql:type:`Object` has a ``__type__`` link
to the ``schema::ObjectType``. For scalars there's the
:eql:op:`introspect` and :eql:op:`typeof` operators that can be used
to get the type of an expression.

Finally, the command :eql:stmt:`describe` can be used to get
information about EdgeDB types in a variety of human-readable formats.

.. toctree::
    :maxdepth: 3
    :hidden:

    objects
    scalars
    colltypes
    functions
    triggers
    mutation_rewrites
    indexes
    constraints
    operators
    casts
