.. _ref_admin_roles:

====
Role
====

:edb-alt-title: Roles


This section describes the administrative commands pertaining to *roles*.


Create role
===========

:eql-statement:

Create a role.

.. eql:synopsis::

    create superuser role <name> [ extending <base> [, ...] ]
    "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      set password := <password>


Description
-----------

The command ``create role`` defines a new database role.

:eql:synopsis:`superuser`
    If specified, the created role will have the *superuser* status, and
    will be exempt from all permission checks.  Currently,
    the ``superuser`` qualifier is mandatory, i.e. it is not possible to
    create non-superuser roles for now.

:eql:synopsis:`<name>`
    The name of the role to create.

:eql:synopsis:`extending <base> [, ...]`
    If specified, declares the parent roles for this role. The role
    inherits all the privileges of the parents.

The following subcommands are allowed in the ``create role`` block:

:eql:synopsis:`set password := <password>`
    Set the password for the role.


Examples
--------

Create a new role:

.. code-block:: edgeql

    create role alice {
        set password := 'wonderland';
    };


Alter role
==========

:eql-statement:

Alter an existing role.

.. eql:synopsis::

    alter role <name> "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      rename to <newname>
      set password := <password>
      extending ...


Description
-----------

The command ``alter role`` changes the settings of an existing role.


:eql:synopsis:`<name>`
    The name of the role to alter.

The following subcommands are allowed in the ``alter role`` block:

:eql:synopsis:`rename to <newname>`
    Change the name of the role to *newname*.

:eql:synopsis:`extending ...`
    Alter the role parent list.  The full syntax of this subcommand is:

    .. eql:synopsis::

         extending <name> [, ...]
            [ first | last | before <parent> | after <parent> ]

    This subcommand makes the role a child of the specified list of
    parent roles. The role inherits all the privileges of the parents.

    It is possible to specify the position in the parent list
    using the following optional keywords:

    * ``first`` -- insert parent(s) at the beginning of the
      parent list,
    * ``last`` -- insert parent(s) at the end of the parent list,
    * ``before <parent>`` -- insert parent(s) before an
      existing *parent*,
    * ``after <parent>`` -- insert parent(s) after an existing
      *parent*.


Examples
--------

Alter a role:

.. code-block:: edgeql

    alter role alice {
        set password := 'new password';
    };


Drop role
=========

:eql-statement:

Remove a role.

.. eql:synopsis::

    drop role <name> ;

Description
-----------

The command ``drop role`` removes an existing role.

Examples
--------

Remove a role:

.. code-block:: edgeql

    drop role alice;
