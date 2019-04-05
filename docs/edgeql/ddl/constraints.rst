.. _ref_eql_ddl_constraints:

===========
Constraints
===========

This section describes the DDL commands pertaining to
:ref:`constraints <ref_datamodel_constraints>`.


CREATE ABSTRACT CONSTRAINT
==========================

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_constraints>` a new abstract constraint.

.. eql:synopsis::

    [ WITH [ <module-alias> := ] MODULE <module-name> ]
    CREATE ABSTRACT CONSTRAINT <name> [ ( [<argspec>] [, ...] ) ]
      [ ON ( <subject-expr> ) ]
      [ EXTENDING <base> [, ...] ]
    "{" <subcommand>; [...] "}" ;

    # where <argspec> is:

      [ $<argname>: ] <argtype>

    # where <subcommand> is one of

      SET expr := <constr-expression>
      SET errmessage := <error-message>
      SET ATTRIBUTE <attribute> := <value>


Description
-----------

``CREATE ABSTRACT CONSTRAINT`` defines a new abstract constraint.

If *name* is qualified with a module name, then the constraint is
created in that module, otherwise it is created in the current module.
The constraint name must be distinct from that of any existing schema item
in the module.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the new constraint.

:eql:synopsis:`<argspec>`
    An optional list of constraint arguments.
    :eql:synopsis:`<argname>` optionally specifies
    the argument name, and :eql:synopsis:`<argtype>`
    specifies the argument type.

:eql:synopsis:`ON ( <subject-expr> )`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the concrete constraint is defined.  The expression must
    refer to the original subject of the constraint as
    ``__subject__``.  Note also that ``<subject-expr>`` itself has to
    be parenthesized.

:eql:synopsis:`EXTENDING <base> [, ...]`
    If specified, declares the *parent* constraints for this constraint.

The following subcommands are allowed in the ``CERATE ABSTRACT
CONSTRAINT`` block:

:eql:synopsis:`SET expr := <constr_expression>`
    A boolean expression that returns ``true`` for valid data and
    ``false`` for invalid data.  The expression may refer to the subject
    of the constraint as ``__subject__``.

:eql:synopsis:`SET errmessage := <error_message>`
    An optional string literal defining the error message template that
    is raised when the constraint is violated.  The template is a formatted
    string that may refer to constraint context variables in curly braces.
    The template may refer to the following:

    - ``$argname`` -- the value of the specified constraint argument
    - ``__subject__`` -- the value of the ``title`` attribute of the scalar
      type, property or link on which the constraint is defined.

:eql:synopsis:`SET ATTRIBUTE <attribute> := <value>;`
    Set constraint *attribute* to *value*.
    See :eql:stmt:`SET ATTRIBUTE` for details.


Examples
--------

Create an abstract constraint "uppercase" which checks if the subject
is a string in upper case.

.. code-block:: edgeql

    CREATE ABSTRACT CONSTRAINT uppercase {
        SET ATTRIBUTE title := "Upper case constraint";
        SET expr := upper(__subject__) = __subject__;
        SET errmessage := "{__subject__} is not in upper case";
    };


ALTER ABSTRACT CONSTRAINT
=========================

:eql-statement:
:eql-haswith:

Alter the definition of an
:ref:`abstract constraint <ref_datamodel_constraints>`.

.. eql:synopsis::

    [ WITH [ <module-alias> := ] MODULE <module-name> ]
    ALTER ABSTRACT CONSTRAINT <name>
    "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      RENAME TO <newname>
      SET expr := <constr-expression>
      SET errmessage := <error-message>
      SET ATTRIBUTE <attribute> := <value>
      DROP ATTRIBUTE <attribute>


Description
-----------

``ALTER ABSTRACT CONSTRAINT`` changes the definition of an abstract constraint
item.  *name* must ve a name of an existing abstract constraint, optionally
qualified with a module name.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the constraint to alter.

The following subcommands are allowed in the ``ALTER ABSTRACT
CONSTRAINT`` block:

:eql:synopsis:`RENAME TO <newname>`
    Change the name of the constraint to *newname*.  All concrete
    constraints inheriting from this constraint are also renamed.

:eql:synopsis:`DROP ATTRIBUTE <attribute>;`
    Remove constraint :eql:synopsis:`<attribute>`.
    See :eql:stmt:`DROP ATTRIBUTE <DROP ATTRIBUTE>` for details.

All the subcommands allowed in the ``CREATE ABSTRACT CONSTRAINT``
block are also valid subcommands for ``ALTER ABSTRACT CONSTRAINT``
block.


DROP ABSTRACT CONSTRAINT
========================

:eql-statement:
:eql-haswith:


Remove an :ref:`abstract constraint <ref_datamodel_constraints>`
from the schema.

.. eql:synopsis::

    [ WITH [ <module-alias> := ] MODULE <module-name> ]
    DROP ABSTRACT CONSTRAINT <name> ;


Description
-----------

``DROP ABSTRACT CONSTRAINT`` removes an existing abstract constraint
item from the database schema.  If any schema items depending on this
constraint exist, the operation is refused.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the constraint to remove.


Examples
--------

Drop abstract constraint ``uppercase``:

.. code-block:: edgeql

    DROP ABSTRACT CONSTRAINT uppercase;


CREATE CONSTRAINT
=================

:eql-statement:

Define a concrete constraint on the specified schema item.

.. eql:synopsis::

    [ WITH [ <module-alias> := ] MODULE <module-name> ]
    CREATE [ DELEGATED ] CONSTRAINT <name>
      [ ( [<argspec>] [, ...] ) ]
      [ ON ( <subject-expr> ) ]
    "{" <subcommand>; [...] "}" ;

    # where <argspec> is:

      [ $<argname>: ] <argtype>

    # where <subcommand> is one of

      SET errmessage := <error-message>
      SET ATTRIBUTE <attribute> := <value>


Description
-----------

``CREATE CONSTRAINT`` defines a new concrete constraint.  It can only be
used in the context of :eql:stmt:`CREATE SCALAR TYPE`,
:eql:stmt:`ALTER SCALAR TYPE`, :eql:stmt:`CREATE PROPERTY`,
:eql:stmt:`ALTER PROPERTY`, :eql:stmt:`CREATE LINK`, or
:eql:Stmt:`ALTER LINK`.

*name* must be a name (optionally module-qualified) of previously defined
abstract constraint.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`DELEGATED`
    If specified, the constraint is defined as *delegated*, which means
    that it will not be enforced on the type it's declared on, and
    the enforcement will be delegated to the subtypes of this type.
    This is particularly useful for :eql:constraint:`exclusive`
    constraints in abstract types.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the abstract constraint
    from which this constraint is derived.

:eql:synopsis:`<argspec>`
    An optional list of constraint arguments.  :eql:synopsis:`<argname>`
    optionally specifies the argument name, and :eql:synopsis:`<argvalue>`
    specifies the argument value.  The argument value specification must
    match the parameter declaration of the abstract constraint.

:eql:synopsis:`ON ( <subject-expr> )`
    An optional expression defining the *subject* of the constraint.
    If not specified, the subject is the value of the schema item on
    which the concrete constraint is defined.  The expression must
    refer to the original subject of the constraint as
    ``__subject__``.  Note also that ``<subject-expr>`` itself has to
    be parenthesized.

    .. note::

        Currently EdgeDB only supports constraint expressions on scalar
        types and properties.

The following subcommands are allowed in the ``CERATE CONSTRAINT`` block:

:eql:synopsis:`SET errmessage := <error_message>`
    An optional string literal defining the error message template that
    is raised when the constraint is violated.  See the relevant
    paragraph in :eql:stmt:`CREATE ABSTRACT CONSTRAINT` for the rules
    of error message template syntax.

:eql:synopsis:`SET <attr-name> := <attr-value>;`
    An optional list of attribute values for the constraint.
    See :eql:stmt:`SET ATTRIBUTE` for details.


Examples
--------

Create a maximum length constraint on the property "name" of the "User" type:

.. code-block:: edgeql

    ALTER TYPE User ALTER PROPERTY name
    CREATE CONSTRAINT max_len_value(100);


ALTER CONSTRAINT
================

:eql-statement:

Alter the definition of a concrete constraint on the specified schema item.

.. eql:synopsis::

    [ WITH [ <module-alias> := ] MODULE <module-name> [, ...] ]
    ALTER CONSTRAINT <name>
    "{" <subcommand>; [ ... ] "}" ;

    # -- or --

    [ WITH [ <module-alias> := ] MODULE <module-name> [, ...] ]
    ALTER CONSTRAINT <name> <subcommand> ;

    # where <subcommand> is one of:

      SET DELEGATED
      DROP DELEGATED
      RENAME TO <newname>
      SET errmessage := <error-message>
      SET ATTRIBUTE <attribute> := <value>
      DROP ATTRIBUTE <attribute>


Description
-----------

``ALTER CONSTRAINT`` changes the definition of a concrete constraint.
As for most ``ALTER`` commands, both single- and multi-command forms are
supported.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the concrete constraint
    that is being altered.

The following subcommands are allowed in the ``ALTER CONSTRAINT`` block:

:eql:synopsis:`SET DELEGATED`
    Makes the constraint delegated.

:eql:synopsis:`DROP DELEGATED`
    Makes the constraint non-delegated.

:eql:synopsis:`RENAME TO <newname>`
    Change the name of the constraint to :eql:synopsis:`<newname>`.

:eql:synopsis:`DROP ATTRIBUTE <attribute>;`
    Remove constraint *attribute*.
    See :eql:stmt:`DROP ATTRIBUTE` for details.

All the subcommands allowed in the ``CREATE CONSTRAINT`` block are also
valid subcommands for ``ALTER CONSTRAINT`` block.

Examples
--------

Change the error message on a maximum length constraint on the property
"name" of the "User" type:

.. code-block:: edgeql

    ALTER TYPE User ALTER PROPERTY name
    ALTER CONSTRAINT max_len_value
    SET errmessage := 'User name too long';


DROP CONSTRAINT
===============

:eql-statement:
:eql-haswith:

Remove a concrete constraint from the specified schema item.

.. eql:synopsis::

    [ WITH [ <module-alias> := ] MODULE <module-name> [, ...] ]
    DROP CONSTRAINT <name>;


Description
-----------

``DROP CONSTRAINT`` removes the specified constraint from its
containing schema item.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] MODULE <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the concrete constraint
    to remove.


Examples
--------

Remove constraint "max_len_value" from the property "name" of the
"User" type:

.. code-block:: edgeql

    ALTER TYPE User ALTER PROPERTY name
    DROP CONSTRAINT max_len_value;
