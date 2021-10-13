
.. _ref_datamodel_modules:

=======
Modules
=======

Your schema consists of several **modules**. Modules have a unique name and can
be used to organize large schemas.

Schemas consist of several kinds of "schema objects", including object types
(equivalent to tables in SQL), functions, scalar types, and expression aliases.
Each of these things has a name. Two schema elements in the same module can't
share the same name.

You can split up your schemas however you see fit. Most users put their entire
schema inside a single module called ``default``.

Name resolution
---------------

When referencing schema objects in a different module, you must use a
*fully-qualified* name of the form ``module_name::object_name``. Consider the
following schema:

.. code-block:: sdl

  module auth {
    type User {
      required property email -> str;
    }
  }

  module data {
    type BlogPost {
      required property title -> str;
      required link author -> auth_module::User;
    }
  }

Note how the ``author`` link inside ``BlogPost`` refers to
``auth_module::User``. If ``User`` and ``BlogPost`` were in the same module
this wouldn't be necessary:

.. code-block:: sdl

  module default {

    type User {
      required property email -> str;
    }

    type BlogPost {
      required property title -> str;
      required link author -> User;
    }

  }

.. important::

  The ``default`` module is special. Fully-qualified names aren't necessary to
  reference schema objects inside ``default``.


Standard modules
----------------

EdgeDB contains the following built-in modules which come pre-populated with
useful types, functions, and operators. These are read-only modules.

* ``std``: standard types, functions and other elements of the
  :ref:`standard library <ref_std>`
* ``schema``: types describing the :ref:`introspection <ref_eql_introspection>`
  schema
* ``sys``: system-wide entities, such as user roles and
  :ref:`databases <ref_datamodel_databases>`
* ``cfg``: configuration and settings
* ``math``: algebraic and statistical :ref:`functions <ref_std_math>`
* ``default``: the default module for user-defined types, functions, etc.
