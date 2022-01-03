
.. _ref_datamodel_modules:

=======
Modules
=======

Every EdgeDB database can contain several **modules**, each with a unique name.
Modules can be used to organize large schemas.

.. code-block:: sdl

  module default {
    type BlogPost {
      required property title -> str;
      required link author -> auth::User;
    }
  }

  module auth {
    type User {
      required property email -> str;
    }
  }

Modules can contain a wide range of *schema elements*: object types (equivalent
to tables in SQL), custom scalar types, expression aliases, abstract links and
properties, functions, and more. Each of these schema elements has a name; no
two elements in the same module can share the same name.

You can split up your schemas however you see fit. Most users put their entire
schema inside a single module called ``default``.

.. _ref_name_resolution:

Fully-qualified names
---------------------

When referencing schema objects in a different module, you must use a
*fully-qualified* name of the form ``module_name::object_name``. In the schema
above, note how ``BlogPost.author`` points to ``auth::User``. If ``User`` and
``BlogPost`` were in the same module the ``auth::`` prefix wouldn't be
necessary.


Standard modules
----------------

EdgeDB contains the following built-in modules which come pre-populated with
useful types, functions, and operators. These are read-only modules. The
contents of these modules are fully documented in :ref:`Standard Library
<ref_std>`.

* ``std``: standard types, functions and other elements of the
  :ref:`standard library <ref_std>`
* ``math``: algebraic and statistical :ref:`functions <ref_std_math>`
* ``cal``: local (non-timezone-aware) and relative date/time :ref:`types and
  functions <ref_std_datetime>`
* ``schema``: types describing the :ref:`introspection <ref_eql_introspection>`
  schema
* ``sys``: system-wide entities, such as user roles and
  :ref:`databases <ref_datamodel_databases>`
* ``cfg``: configuration and settings

