.. _ref_admin_roles:

=====
Roles
=====

This section describes the administrative commands pertaining to *roles*.


CREATE ROLE
===========

:eql-statement:

Create a role.

.. eql:synopsis::

    CREATE ROLE <name> [ EXTENDING <base> [, ...] ]
    "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      SET allow_login := {true | false}
      SET is_superuser := {true | false}
      SET password := <password>
      SET ANNOTATION <annotation-name> := <value>


Description
-----------

``CREATE ROLE`` defines a new role.

:eql:synopsis:`<name>`
    The name of the role to create.

:eql:synopsis:`EXTENDING <base> [, ...]`
    If specified, declares the *parent* roles for this role.

The following subcommands are allowed in the ``CREATE ROLE`` block:

:eql:synopsis:`SET allow_login := {true | false}`
    A boolean flag that controls whether this role can be used to log
    into EdgeDB. It is ``false`` by default.

:eql:synopsis:`SET is_superuser := {true | false}`
    A boolean flag that determines whether this role is a "superuser".
    A "superuser" can override all access restrictions within EdgeDB.
    It is ``false`` by default.

:eql:synopsis:`SET password := <password>`
    Set the password for the role.

:eql:synopsis:`SET ANNOTATION <annotation-name> := <value>;`
    Set role :eql:synopsis:`<annotation-name>` to :eql:synopsis:`<value>`.
    See :eql:stmt:`SET ANNOTATION` for details.


Examples
--------

Create a new role:

.. code-block:: edgeql

    CREATE ROLE alice {
        SET allow_login := true;
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
      SET allow_login := {true | false}
      SET is_superuser := {true | false}
      SET password := <password>
      SET ANNOTATION <annotation-name> := <value>
      DROP ANNOTATION <annotation-name>


Description
-----------

``ALTER ROLE`` changes the settings of an existing role.


:eql:synopsis:`<name>`
    The name of the role to alter.

The following subcommands are allowed in the ``ALTER ROLE`` block:

:eql:synopsis:`RENAME TO <newname>`
    Change the name of the role to *newname*.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove role :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`DROP ANNOTATION <DROP ANNOTATION>` for details.

All the subcommands allowed in the ``CREATE ROLE`` block are also
valid subcommands for ``ALTER ROLE`` block.


Examples
--------

Alter a role:

.. code-block:: edgeql

    ALTER ROLE alice {
        SET allow_login := false;
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
