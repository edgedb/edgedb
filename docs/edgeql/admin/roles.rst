.. _ref_admin_roles:

====
ROLE
====

:edb-alt-title: Roles


This section describes the administrative commands pertaining to *roles*.


CREATE ROLE
===========

:eql-statement:

Create a role.

.. eql:synopsis::

    CREATE SUPERUSER ROLE <name> [ EXTENDING <base> [, ...] ]
    "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      SET password := <password>


Description
-----------

``CREATE ROLE`` defines a new database role.

:eql:synopsis:`SUPERUSER`
    If specified, the created role will have the *superuser* status, and
    will be exempt from all permission checks.  Currently,
    the ``SUPERUSER`` qualifier is mandatory, i.e. it is not possible to
    create non-superuser roles for now.

:eql:synopsis:`<name>`
    The name of the role to create.

:eql:synopsis:`EXTENDING <base> [, ...]`
    If specified, declares the parent roles for this role. The role
    inherits all the privileges of the parents.

The following subcommands are allowed in the ``CREATE ROLE`` block:

:eql:synopsis:`SET password := <password>`
    Set the password for the role.


Examples
--------

Create a new role:

.. code-block:: edgeql

    CREATE ROLE alice {
        SET password := 'wonderland';
    };


ALTER ROLE
==========

:eql-statement:

Alter an existing role.

.. eql:synopsis::

    ALTER ROLE <name> "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      RENAME TO <newname>
      SET password := <password>
      EXTENDING ...


Description
-----------

``ALTER ROLE`` changes the settings of an existing role.


:eql:synopsis:`<name>`
    The name of the role to alter.

The following subcommands are allowed in the ``ALTER ROLE`` block:

:eql:synopsis:`RENAME TO <newname>`
    Change the name of the role to *newname*.

:eql:synopsis:`EXTENDING ...`
    Alter the role parent list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         EXTENDING <name> [, ...]
            [ FIRST | LAST | BEFORE <parent> | AFTER <parent> ]

    This subcommand makes the role a child of the specified list of
    parent roles. The role inherits all the privileges of the parents.

    It is possible to specify the position in the parent list
    using the following optional keywords:

    * ``FIRST`` -- insert parent(s) at the beginning of the
      parent list,
    * ``LAST`` -- insert parent(s) at the end of the parent list,
    * ``BEFORE <parent>`` -- insert parent(s) before an
      existing *parent*,
    * ``AFTER <parent>`` -- insert parent(s) after an existing
      *parent*.


Examples
--------

Alter a role:

.. code-block:: edgeql

    ALTER ROLE alice {
        SET password := 'new password';
    };


DROP ROLE
=========

:eql-statement:

Remove a role.

.. eql:synopsis::

    DROP ROLE <name> ;

Description
-----------

``DROP ROLE`` removes an existing role.

Examples
--------

Remove a role:

.. code-block:: edgeql

    DROP ROLE alice;
