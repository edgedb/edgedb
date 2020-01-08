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

      USING <constr-expression>
      SET errmessage := <error-message>
      CREATE ANNOTATION <annotation-name> := <value>


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

:eql:synopsis:`USING <constr_expression>`
    A boolean expression that returns ``true`` for valid data and
    ``false`` for invalid data.  The expression may refer to the subject
    of the constraint as ``__subject__``.

:eql:synopsis:`SET errmessage := <error_message>`
    An optional string literal defining the error message template that
    is raised when the constraint is violated.  The template is a formatted
    string that may refer to constraint context variables in curly braces.
    The template may refer to the following:

    - ``$argname`` -- the value of the specified constraint argument
    - ``__subject__`` -- the value of the ``title`` annotation of the scalar
      type, property or link on which the constraint is defined.

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>;`
    Set constraint :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`CREATE ANNOTATION` for details.


Example
-------

Create an abstract constraint "uppercase" which checks if the subject
is a string in upper case.

.. code-block:: edgeql

    CREATE ABSTRACT CONSTRAINT uppercase {
        CREATE ANNOTATION title := "Upper case constraint";
        USING (str_upper(__subject__) = __subject__);
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
      USING <constr-expression>
      SET errmessage := <error-message>
      CREATE ANNOTATION <annotation-name> := <value>
      ALTER ANNOTATION <annotation-name> := <value>
      DROP ANNOTATION <annotation-name>


Description
-----------

``ALTER ABSTRACT CONSTRAINT`` changes the definition of an abstract constraint
item.  *name* must be a name of an existing abstract constraint, optionally
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

:eql:synopsis:`ALTER ANNOTATION <annotation-name>;`
    Alter constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`ALTER ANNOTATION <ALTER ANNOTATION>` for details.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`DROP ANNOTATION <DROP ANNOTATION>` for details.

All the subcommands allowed in the ``CREATE ABSTRACT CONSTRAINT``
block are also valid subcommands for ``ALTER ABSTRACT CONSTRAINT``
block.


Example
-------

Rename the abstract constraint "uppercase"  to "upper_case":

.. code-block:: edgeql

    ALTER ABSTRACT CONSTRAINT uppercase RENAME TO upper_case;


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


Example
-------

Drop abstract constraint ``upper_case``:

.. code-block:: edgeql

    DROP ABSTRACT CONSTRAINT upper_case;


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
      CREATE ANNOTATION <annotation-name> := <value>


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

:eql:synopsis:`CREATE ANNOTATION <annotation-name> := <value>;`
    An optional list of annotations for the constraint.
    See :eql:stmt:`CREATE ANNOTATION` for details.


Example
-------

Create a "score" property on the "User" type with a minimum value
constraint:

.. code-block:: edgeql

    ALTER TYPE User CREATE PROPERTY score -> int64 {
        CREATE CONSTRAINT min_value(0)
    };


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
      CREATE ANNOTATION <annotation-name> := <value>
      ALTER ANNOTATION <annotation-name>
      DROP ANNOTATION <annotation-name>


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

:eql:synopsis:`ALTER ANNOTATION <annotation-name>;`
    Alter constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`ALTER ANNOTATION <ALTER ANNOTATION>` for details.

:eql:synopsis:`DROP ANNOTATION <annotation-name>;`
    Remove an *annotation*. See :eql:stmt:`DROP ANNOTATION` for details.

All the subcommands allowed in the ``CREATE CONSTRAINT`` block are also
valid subcommands for ``ALTER CONSTRAINT`` block.

Example
-------

Change the error message on the minimum value constraint on the property
"score" of the "User" type:

.. code-block:: edgeql

    ALTER TYPE User ALTER PROPERTY score
    ALTER CONSTRAINT min_value
    SET errmessage := 'Score cannot be negative';


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


Example
-------

Remove constraint "min_value" from the property "score" of the
"User" type:

.. code-block:: edgeql

    ALTER TYPE User ALTER PROPERTY score
    DROP CONSTRAINT min_value;
