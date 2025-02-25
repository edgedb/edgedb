.. _ref_datamodel_future:

===============
Future behavior
===============

.. index:: future, nonrecursive_access_policies

This article explains what the ``using future ...;`` statement means in your
schema.

Our goal is to make |Gel| the best database system in the world, which requires
us to keep evolving. Usually, we can add new functionality while preserving
backward compatibility, but on rare occasions we must implement changes that
require elaborate transitions.

To handle these cases, we introduce *future* behavior, which lets you try out
upcoming features before a major release. Sometimes enabling a future is
necessary to fix current issues; other times it offers a safe and easy way to
ensure your codebase remains compatible. This approach provides more time to
adopt a new feature and identify any resulting bugs.

Any time a behavior is available as a ``future,`` all new :ref:`projects
<ref_intro_projects>` enable it by default for empty databases. You can remove
a ``future`` from your schema if absolutely necessary, but doing so is
discouraged. Existing projects are unaffected by default, so you must manually
add the ``future`` specification to gain early access.

Flags
=====

At the moment there are three ``future`` flags available:

- ``simple_scoping``

  Introduced in |Gel| 6.0, this flag simplifies the scoping rules for
  path expressions. Read more about it and in great detail in
  :ref:`ref_eql_path_resolution`.

- ``warn_old_scoping``

  Introduced in |Gel| 6.0, this flag will emit a warning when a query
  is detected to depend on the old scoping rules. This is an intermediate
  step towards enabling the ``simple_scoping`` flag in existing large
  codebases.

  Read more about this flag in :ref:`ref_warn_old_scoping`.

.. _ref_datamodel_access_policies_nonrecursive:
.. _nonrecursive:

- ``nonrecursive_access_policies``: makes access policies non-recursive.

  This flag is no longer used becauae the behavior is enabled
  by default since |EdgeDB| 4. The flag was helpful to ease transition
  from EdgeDB 3.x to 4.x.

  Since |EdgeDB| 3.0, access policy restrictions do **not** apply
  to any access policy expression. This means that when reasoning about access
  policies it is no longer necessary to take other policies into account.
  Instead, all data is visible for the purpose of *defining* an access
  policy.

  This change was made to simplify reasoning about access policies and
  to allow certain patterns to be expressed efficiently. Since those who have
  access to modifying the schema can remove unwanted access policies, no
  additional security is provided by applying access policies to each
  other's expressions.


.. _ref_eql_sdl_future:

Declaring future flags
======================

Syntax
------

Declare that the current schema enables a particular future behavior.

.. sdl:synopsis::

  using future <FutureBehavior> ";"

Description
^^^^^^^^^^^

Future behavior declaration must be outside any :ref:`module block
<ref_eql_sdl_modules>` since this behavior affects the entire database and not
a specific module.

Example
^^^^^^^

.. code-block:: sdl-invalid

  using future simple_scoping;


.. _ref_eql_ddl_future:

DDL commands
============

This section describes the low-level DDL commands for creating and
dropping future flags. You typically don't need to use these commands directly,
but knowing about them is useful for reviewing migrations.

Create future
-------------

:eql-statement:

Enable a particular future behavior for the current schema.

.. eql:synopsis::

  create future <FutureBehavior> ";"


The command ``create future`` enables the specified future behavior for
the current branch.

Example
^^^^^^^

.. code-block:: edgeql

  create future simple_scoping;


Drop future
-----------

:eql-statement:

Disable a particular future behavior for the current schema.

.. eql:synopsis::

  drop future <FutureBehavior> ";"

Description
^^^^^^^^^^^

The command ``drop future`` disables a currently active future behavior for the
current branch. However, this is only possible for versions of |Gel| when the
behavior in question is not officially introduced. Once a particular behavior is
introduced as the standard behavior in a |Gel| release, it cannot be disabled.

Example
^^^^^^^

.. code-block:: edgeql

  drop future warn_old_scoping;
