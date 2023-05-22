.. _ref_eql_ddl_constraints:

===========
Constraints
===========

This section describes the DDL commands pertaining to
:ref:`constraints <ref_datamodel_constraints>`.


Create abstract constraint
==========================

:eql-statement:
:eql-haswith:

:ref:`Define <ref_eql_sdl_constraints>` a new abstract constraint.

.. eql:synopsis::

    [ with [ <module-alias> := ] module <module-name> ]
    create abstract constraint <name> [ ( [<argspec>] [, ...] ) ]
      [ on ( <subject-expr> ) ]
      [ extending <base> [, ...] ]
    "{" <subcommand>; [...] "}" ;

    # where <argspec> is:

      [ <argname>: ] <argtype>

    # where <subcommand> is one of

      using <constr-expression>
      set errmessage := <error-message>
      create annotation <annotation-name> := <value>


Description
-----------

The command ``create abstract constraint`` defines a new abstract constraint.

If *name* is qualified with a module name, then the constraint is
created in that module, otherwise it is created in the current module.
The constraint name must be distinct from that of any existing schema item
in the module.


Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL constraint declaration <ref_eql_sdl_constraints_syntax>`,
with some additional features listed below:

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`set errmessage := <error_message>`
    An optional string literal defining the error message template
    that is raised when the constraint is violated. Other than a
    slight syntactical difference this is the same as the
    corresponding SDL declaration.

:eql:synopsis:`create annotation <annotation-name> := <value>;`
    Set constraint :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.


Example
-------

Create an abstract constraint "uppercase" which checks if the subject
is a string in upper case.

.. code-block:: edgeql

    create abstract constraint uppercase {
        create annotation title := "Upper case constraint";
        using (str_upper(__subject__) = __subject__);
        set errmessage := "{__subject__} is not in upper case";
    };


Alter abstract constraint
=========================

:eql-statement:
:eql-haswith:

Alter the definition of an
:ref:`abstract constraint <ref_datamodel_constraints>`.

.. eql:synopsis::

    [ with [ <module-alias> := ] module <module-name> ]
    alter abstract constraint <name>
    "{" <subcommand>; [...] "}" ;

    # where <subcommand> is one of

      rename to <newname>
      using <constr-expression>
      set errmessage := <error-message>
      reset errmessage
      create annotation <annotation-name> := <value>
      alter annotation <annotation-name> := <value>
      drop annotation <annotation-name>


Description
-----------

The command ``alter abstract constraint`` changes the definition of an
abstract constraint item.  *name* must be a name of an existing
abstract constraint, optionally qualified with a module name.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the constraint to alter.

The following subcommands are allowed in the ``alter abstract
constraint`` block:

:eql:synopsis:`rename to <newname>`
    Change the name of the constraint to *newname*.  All concrete
    constraints inheriting from this constraint are also renamed.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`reset errmessage;`
    Remove the error message from this abstract constraint.
    The error message specified in the base abstract constraint
    will be used instead.

All the subcommands allowed in a ``create abstract constraint`` block
are also valid subcommands for an ``alter abstract constraint`` block.


Example
-------

Rename the abstract constraint "uppercase"  to "upper_case":

.. code-block:: edgeql

    alter abstract constraint uppercase rename to upper_case;


Drop abstract constraint
========================

:eql-statement:
:eql-haswith:


Remove an :ref:`abstract constraint <ref_datamodel_constraints>`
from the schema.

.. eql:synopsis::

    [ with [ <module-alias> := ] module <module-name> ]
    drop abstract constraint <name> ;


Description
-----------

The command ``drop abstract constraint`` removes an existing abstract
constraint item from the database schema.  If any schema items
depending on this constraint exist, the operation is refused.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
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

    drop abstract constraint upper_case;


Create constraint
=================

:eql-statement:

Define a concrete constraint on the specified schema item.

.. eql:synopsis::

    [ with [ <module-alias> := ] module <module-name> ]
    create [ delegated ] constraint <name>
      [ ( [<argspec>] [, ...] ) ]
      [ on ( <subject-expr> ) ]
      [ except ( <except-expr> ) ]
    "{" <subcommand>; [...] "}" ;

    # where <argspec> is:

      [ <argname>: ] <argvalue>

    # where <subcommand> is one of

      set errmessage := <error-message>
      create annotation <annotation-name> := <value>


Description
-----------

The command ``create constraint`` defines a new concrete constraint.
It can only be used in the context of :eql:stmt:`create scalar type`,
:eql:stmt:`alter scalar type`, :eql:stmt:`create property`,
:eql:stmt:`alter property`, :eql:stmt:`create link`, or
:eql:Stmt:`alter link`.

*name* must be a name (optionally module-qualified) of previously defined
abstract constraint.


Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL constraint declaration <ref_eql_sdl_constraints_syntax>`,
with some additional features listed below:

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`set errmessage := <error_message>`
    An optional string literal defining the error message template
    that is raised when the constraint is violated. Other than a
    slight syntactical difference this is the same as the
    corresponding SDL declaration.

:eql:synopsis:`create annotation <annotation-name> := <value>;`
    An optional list of annotations for the constraint.
    See :eql:stmt:`create annotation` for details.


Example
-------

Create a "score" property on the "User" type with a minimum value
constraint:

.. code-block:: edgeql

    alter type User create property score -> int64 {
        create constraint min_value(0)
    };

Create a Vector with a maximum magnitude:

.. code-block:: edgeql

    create type Vector {
        create required property x -> float64;
        create required property y -> float64;
        create constraint expression ON (
            __subject__.x^2 + __subject__.y^2 < 25
        );
    }


Alter constraint
================

:eql-statement:

Alter the definition of a concrete constraint on the specified schema item.

.. eql:synopsis::

    [ with [ <module-alias> := ] module <module-name> [, ...] ]
    alter constraint <name>
      [ ( [<argspec>] [, ...] ) ]
      [ on ( <subject-expr> ) ]
      [ except ( <except-expr> ) ]
    "{" <subcommand>; [ ... ] "}" ;

    # -- or --

    [ with [ <module-alias> := ] module <module-name> [, ...] ]
    alter constraint <name>
      [ ( [<argspec>] [, ...] ) ]
      [ on ( <subject-expr> ) ]
      <subcommand> ;

    # where <subcommand> is one of:

      set delegated
      set not delegated
      set errmessage := <error-message>
      reset errmessage
      create annotation <annotation-name> := <value>
      alter annotation <annotation-name>
      drop annotation <annotation-name>


Description
-----------

The command ``alter constraint`` changes the definition of a concrete
constraint. As for most ``alter`` commands, both single- and
multi-command forms are supported.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the concrete constraint
    that is being altered.

:eql:synopsis:`<argspec>`
    A list of constraint arguments as specified at the time of
    ``create constraint``.

:eql:synopsis:`on ( <subject-expr> )`
    A expression defining the *subject* of the constraint as specified
    at the time of ``create constraint``.


The following subcommands are allowed in the ``alter constraint`` block:

:eql:synopsis:`set delegated`
    If set, the constraint is defined as *delegated*, which means that it will
    not be enforced on the type it's declared on, and the enforcement will be
    delegated to the subtypes of this type. This is particularly useful for
    :eql:constraint:`exclusive` constraints in abstract types. This is only
    valid for *concrete constraints*.

:eql:synopsis:`set not delegated`
    If set, the constraint is defined as *not delegated*, which means that it
    will be enforced globally across the type it's declared on and any
    extending types.

:eql:synopsis:`rename to <newname>`
    Change the name of the constraint to :eql:synopsis:`<newname>`.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter constraint :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove an *annotation*. See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`reset errmessage;`
    Remove the error message from this constraint. The error message
    specified in the abstract constraint will be used instead.

All the subcommands allowed in the ``create constraint`` block are also
valid subcommands for ``alter constraint`` block.

Example
-------

Change the error message on the minimum value constraint on the property
"score" of the "User" type:

.. code-block:: edgeql

    alter type User alter property score
        alter constraint min_value(0)
            set errmessage := 'Score cannot be negative';


Drop constraint
===============

:eql-statement:
:eql-haswith:

Remove a concrete constraint from the specified schema item.

.. eql:synopsis::

    [ with [ <module-alias> := ] module <module-name> [, ...] ]
    drop constraint <name>
      [ ( [<argspec>] [, ...] ) ]
      [ on ( <subject-expr> ) ]
      [ except ( <except-expr> ) ] ;


Description
-----------

The command ``drop constraint`` removes the specified constraint from
its containing schema item.


Parameters
----------

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
    An optional list of module alias declarations to be used in the
    migration definition.  When *module-alias* is not specified,
    *module-name* becomes the effective current module and is used
    to resolve all unqualified names.

:eql:synopsis:`<name>`
    The name (optionally module-qualified) of the concrete constraint
    to remove.

:eql:synopsis:`<argspec>`
    A list of constraint arguments as specified at the time of
    ``create constraint``.

:eql:synopsis:`on ( <subject-expr> )`
    A expression defining the *subject* of the constraint as specified
    at the time of ``create constraint``.


Example
-------

Remove constraint "min_value" from the property "score" of the
"User" type:

.. code-block:: edgeql

    alter type User alter property score
        drop constraint min_value(0);


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Constraints <ref_datamodel_constraints>`
  * - :ref:`SDL > Constraints <ref_eql_sdl_constraints>`
  * - :ref:`Introspection > Constraints
      <ref_datamodel_introspection_constraints>`
  * - :ref:`Standard Library > Constraints <ref_std_constraints>`
  * - `Tutorial > Advanced EdgeQL > Constraints
      </tutorial/advanced-edgeql/constraints>`_
