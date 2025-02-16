.. _ref_datamodel_constraints:
.. _ref_eql_sdl_constraints:

===========
Constraints
===========

.. index:: constraint, validation, exclusive, expression on, one_of, max_value,
           max_ex_value, min_value, min_ex_value, max_len_value, min_len_value,
           regexp, __subject__

Constraints give users fine-grained control to ensure data consistency.
They can be defined on :ref:`properties <ref_datamodel_props>`,
:ref:`links<ref_datamodel_links>`,
:ref:`object types <ref_datamodel_object_types>`,
and :ref:`custom scalars <ref_datamodel_links>`.


.. _ref_datamodel_constraints_builtin:

Standard constraints
====================

|Gel| includes a number of standard ready-to-use constraints:

.. include:: ../stdlib/constraint_table.rst


Constraints on properties
=========================

.. code-block:: sdl

  type User {
    required username: str {
      # usernames must be unique
      constraint exclusive;

      # max length (built-in)
      constraint max_len_value(25);
    };
  }


Custom constraints
==================

The ``expression`` constraint is used to define custom constraint logic. Inside
custom constraints, the keyword ``__subject__`` can be used to reference the
*value* being constrained.

.. code-block:: sdl

  type User {
    required username: str {
      # max length (as custom constraint)
      constraint expression on (len(__subject__) <= 25);
    };
  }


.. _ref_datamodel_constraints_objects:

Constraints on object types
===========================

Constraints can be defined on object types. This is useful when the
constraint logic must reference multiple links or properties.

.. important::

  Inside an object type declaration, you can omit ``__subject__`` and simply
  refer to properties with the :ref:`leading dot notation <ref_dot_notation>`
  (e.g. ``.<name>``).

.. code-block:: sdl

  type ConstrainedVector {
    required x: float64;
    required y: float64;

    constraint expression on (
      .x ^ 2 + .y ^ 2 <= 25
    );
  }

Note that the constraint expression are fairly restricted. Due
to how constraints are implemented, you can only reference ``single``
(non-multi) properties and links defined on the object type:

.. code-block:: sdl

  # Not valid!
  type User {
    required username: str;
    multi friends: User;

    # ❌ constraints cannot contain paths with more than one hop
    constraint expression on ('bob' in .friends.username);
  }

Abstract constraints
====================

.. code-block:: sdl

    abstract constraint min_value(min: anytype) {
        errmessage :=
          'Minimum allowed value for {__subject__} is {min}.';

        using (__subject__ >= min);
    }

    # use it like this:

    scalar type posint64 extending int64 {
        constraint min_value(0);
    }

    # or like this:

    type User {
      required age: int16 {
        constraint min_value(12);
      };
    }


Computed constraints
====================

Constraints can be defined on computed properties:

.. code-block:: sdl

  type User {
    required username: str;
    required clean_username := str_trim(str_lower(.username));

    constraint exclusive on (.clean_username);
  }


Composite constraints
=====================

.. index:: tuple

To define a composite constraint, create an ``exclusive`` constraint on a
tuple of properties or links.

.. code-block:: sdl

  type User {
    username: str;
  }

  type BlogPost {
    title: str;

    author: User;

    constraint exclusive on ((.title, .author));
  }

.. _ref_datamodel_constraints_partial:

Partial constraints
===================

.. index:: constraint exclusive on, except

Constraints on object types can be made partial, so that they are not enforced
when the specified ``except`` condition is met.

.. code-block:: sdl

  type User {
    required username: str;
    deleted: bool;

    # Usernames must be unique unless marked deleted
    constraint exclusive on (.username) except (.deleted);
  }


Constraints on links
====================

You can constrain links such that a given object can only be linked once by
using :eql:constraint:`exclusive`:

.. code-block:: sdl

  type User {
    required name: str;

    # Make sure none of the "owned" items belong
    # to any other user.
    multi owns: Item {
      constraint exclusive;
    }
  }


Link property constraints
=========================

You can also add constraints for :ref:`link properties
<ref_datamodel_link_properties>`:

.. code-block:: sdl

  type User {
    name: str;

    multi friends: User {
      strength: float64;

      constraint expression on (
        @strength >= 0
      );
    }
  }


Link's "@source" and "@target"
==============================

.. index:: constraint exclusive on, @source, @target

You can create a composite exclusive constraint on the object linking/linked
*and* a link property by using ``@source`` or ``@target`` respectively. Here's
a schema for a library book management app that tracks books and who has
checked them out:

.. code-block:: sdl

  type Book {
    required title: str;
  }

  type User {
    name: str;
    multi checked_out: Book {
      date: cal::local_date;

      # Ensures a given Book can be checked out
      # only once on a given day.
      constraint exclusive on ((@target, @date));
    }
  }

Here, the constraint ensures that no book can be checked out to two ``User``\s
on the same ``@date``.

In this example demonstrating ``@source``, we've created a schema to track
player picks in a color-based memory game:

.. code-block:: sdl

  type Player {
    required name: str;

    multi picks: Color {
      order: int16;

      constraint exclusive on ((@source, @order));
    }
  }

  type Color {
    required name: str;
  }

This constraint ensures that a single ``Player`` cannot pick two ``Color``\s at
the same ``@order``.


Constraints on custom scalars
=============================

Custom scalar types can be constrained.

.. code-block:: sdl

  scalar type username extending str {
    constraint regexp(r'^[A-Za-z0-9_]{4,20}$');
  }

Note: you can't use :eql:constraint:`exclusive` constraints on custom scalar
types, as the concept of exclusivity is only defined in the context of a given
object type.

Use :eql:constraint:`expression` constraints to declare custom constraints
using arbitrary EdgeQL expressions. The example below uses the built-in
:eql:func:`str_trim` function.

.. code-block:: sdl

  scalar type title extending str {
    constraint expression on (
      __subject__ = str_trim(__subject__)
    );
  }


Constraints and inheritance
===========================

.. index:: delegated constraint

If you define a constraint on a type and then extend that type, the constraint
will *not* be applied individually to each extending type. Instead, it will
apply globally across all the types that inherited the constraint.

.. code-block:: sdl

  type User {
    required name: str {
      constraint exclusive;
    }
  }
  type Administrator extending User;
  type Moderator extending User;

.. code-block:: edgeql-repl

  gel> insert Administrator {
  ....  name := 'Jan'
  .... };
  {default::Administrator {id: 7aeaa146-f5a5-11ed-a598-53ddff476532}}

  gel> insert Moderator {
  ....  name := 'Jan'
  .... };
  gel error: ConstraintViolationError: name violates exclusivity constraint
    Detail: value of property 'name' of object type 'default::Moderator'
    violates exclusivity constraint

  gel> insert User {
  ....  name := 'Jan'
  .... };
  gel error: ConstraintViolationError: name violates exclusivity constraint
    Detail: value of property 'name' of object type 'default::User'
    violates exclusivity constraint

As this example demonstrates, if an object of one extending type has a value
for a property that is exclusive, an object of a *different* extending type
cannot have the same value.

If that's not what you want, you can instead delegate the constraint to the
inheriting types by prepending the ``delegated`` keyword to the constraint.
The constraint would then be applied just as if it were declared individually
on each of the inheriting types.

.. code-block:: sdl

  type User {
    required name: str {
      delegated constraint exclusive;
    }
  }
  type Administrator extending User;
  type Moderator extending User;

.. code-block:: edgeql-repl

  gel> insert Administrator {
  ....  name := 'Jan'
  .... };
  {default::Administrator {id: 7aeaa146-f5a5-11ed-a598-53ddff476532}}

  gel> insert User {
  ....  name := 'Jan'
  .... };
  {default::User {id: a6e3fdaf-c44b-4080-b39f-6a07496de66b}}

  gel> insert Moderator {
  ....  name := 'Jan'
  .... };
  {default::Moderator {id: d3012a3f-0f16-40a8-8884-7203f393b63d}}

  gel> insert Moderator {
  ....  name := 'Jan'
  .... };
  gel error: ConstraintViolationError: name violates exclusivity constraint
    Detail: value of property 'name' of object type 'default::Moderator'
    violates exclusivity constraint

With the addition of ``delegated`` to the constraints, the inserts were
successful for each of the types. We did not hit a constraint violation
until we tried to insert a second ``Moderator`` object with the same
name as the existing one.


.. _ref_eql_sdl_constraints_syntax:

Declaring constraints
=====================

This section describes the syntax to declare constraints in your schema.

Syntax
------

.. sdl:synopsis::

  [{abstract | delegated}] constraint <name> [ ( [<argspec>] [, ...] ) ]
      [ on ( <subject-expr> ) ]
      [ except ( <except-expr> ) ]
      [ extending <base> [, ...] ]
  "{"
      [ using <constr-expression> ; ]
      [ errmessage := <error-message> ; ]
      [ <annotation-declarations> ]
      [ ... ]
  "}" ;

  # where <argspec> is:

  [ <argname>: ] {<argtype> | <argvalue>}

Description
^^^^^^^^^^^

This declaration defines a new constraint with the following options:

:eql:synopsis:`abstract`
  If specified, the constraint will be *abstract*.

:eql:synopsis:`delegated`
  If specified, the constraint is defined as *delegated*, which means
  that it will not be enforced on the type it's declared on, and the
  enforcement will be delegated to the subtypes of this type.
  This is particularly useful for :eql:constraint:`exclusive`
  constraints in abstract types. This is only valid for *concrete
  constraints*.

:eql:synopsis:`<name>`
  The name (optionally module-qualified) of the new constraint.

:eql:synopsis:`<argspec>`
  An optional list of constraint arguments.

  For an *abstract constraint* :eql:synopsis:`<argname>` optionally
  specifies the argument name and :eql:synopsis:`<argtype>` specifies
  the argument type.

  For a *concrete constraint* :eql:synopsis:`<argname>` optionally
  specifies the argument name and :eql:synopsis:`<argvalue>` specifies
  the argument value. The argument value specification must match the
  parameter declaration of the abstract constraint.

:eql:synopsis:`on ( <subject-expr> )`
  An optional expression defining the *subject* of the constraint.
  If not specified, the subject is the value of the schema item on which
  the concrete constraint is defined.

  The expression must refer to the original subject of the constraint as
  ``__subject__``. The expression must be
  :ref:`Immutable <ref_reference_volatility>`, but may refer to
  ``__subject__`` and its properties and links.

  Note also that ``<subject-expr>`` itself has to
  be parenthesized.

:eql:synopsis:`except ( <exception-expr> )`
  An optional expression defining a condition to create exceptions to
  the constraint. If ``<exception-expr>`` evaluates to ``true``,
  the constraint is ignored for the current subject. If it evaluates
  to ``false`` or ``{}``, the constraint applies normally.

  ``except`` may only be declared on object constraints, and otherwise
  follows the same rules as ``on``.

:eql:synopsis:`extending <base> [, ...]`
  If specified, declares the *parent* constraints for this abstract
  constraint.

The valid SDL sub-declarations are listed below:

:eql:synopsis:`using <constr_expression>`
  A boolean expression that returns ``true`` for valid data and
  ``false`` for invalid data. The expression may refer to the
  subject of the constraint as ``__subject__``. This declaration is
  only valid for *abstract constraints*.

:eql:synopsis:`errmessage := <error_message>`
  An optional string literal defining the error message template
  that is raised when the constraint is violated. The template is a
  formatted string that may refer to constraint context variables in
  curly braces. The template may refer to the following:

  - ``$argname`` -- the value of the specified constraint argument
  - ``__subject__`` -- the value of the ``title`` annotation of the
    scalar type, property or link on which the constraint is defined.

  If the content of curly braces does not match any variables,
  the curly braces are emitted as-is. They can also be escaped by
  using double curly braces.

:sdl:synopsis:`<annotation-declarations>`
  Set constraint :ref:`annotation <ref_eql_sdl_annotations>`
  to a given *value*.


.. _ref_eql_ddl_constraints:

DDL commands
============

This section describes the low-level DDL commands for creating and dropping
constraints and abstract constraints. You typically don't need to use these
commands directly, but knowing about them is useful for reviewing migrations.


Create abstract constraint
--------------------------

:eql-statement:
:eql-haswith:

Define a new abstract constraint.

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
^^^^^^^^^^^
The command ``create abstract constraint`` defines a new abstract constraint.

If *name* is qualified with a module name, then the constraint is created in
that module, otherwise it is created in the current module. The constraint
name must be distinct from that of any existing schema item in the module.


Parameters
^^^^^^^^^^
Most sub-commands and options of this command are identical to the
:ref:`SDL constraint declaration <ref_eql_sdl_constraints_syntax>`,
with some additional features listed below:

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
  An optional list of module alias declarations to be used in the
  migration definition. When *module-alias* is not specified,
  *module-name* becomes the effective current module and is used
  to resolve all unqualified names.

:eql:synopsis:`set errmessage := <error_message>`
  An optional string literal defining the error message template
  that is raised when the constraint is violated. Other than a
  slight syntactical difference this is the same as the
  corresponding SDL declaration.

:eql:synopsis:`create annotation <annotation-name> := <value>;`
  Set constraint annotation ``<annotation-name>`` to ``<value>``.
  See :eql:stmt:`create annotation` for details.


Example
^^^^^^^
Create an abstract constraint "uppercase" which checks if the subject
is a string in upper case:

.. code-block:: edgeql

  create abstract constraint uppercase {
    create annotation title := "Upper case constraint";

    using (str_upper(__subject__) = __subject__);

    set errmessage := "{__subject__} is not in upper case";
  };


Alter abstract constraint
-------------------------

:eql-statement:
:eql-haswith:

Alter the definition of an abstract constraint.

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
^^^^^^^^^^^

The command ``alter abstract constraint`` changes the definition of an
abstract constraint item. *name* must be a name of an existing
abstract constraint, optionally qualified with a module name.


Parameters
^^^^^^^^^^

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
  An optional list of module alias declarations to be used in the
  migration definition. When *module-alias* is not specified,
  *module-name* becomes the effective current module and is used
  to resolve all unqualified names.

:eql:synopsis:`<name>`
  The name (optionally module-qualified) of the constraint to alter.

Subcommands allowed in the ``alter abstract constraint`` block:

:eql:synopsis:`rename to <newname>`
  Change the name of the constraint to *newname*. All concrete
  constraints inheriting from this constraint are also renamed.

:eql:synopsis:`alter annotation <annotation-name> := <value>`
  Alter constraint annotation ``<annotation-name>``.
  See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>`
  Remove annotation ``<annotation-name>``.
  See :eql:stmt:`drop annotation` for details.

:eql:synopsis:`reset errmessage`
  Remove the error message from this abstract constraint. The error message
  specified in the base abstract constraint will be used instead.

All subcommands allowed in a ``create abstract constraint`` block are also
valid here.


Example
^^^^^^^

Rename the abstract constraint "uppercase" to "upper_case":

.. code-block:: edgeql

  alter abstract constraint uppercase rename to upper_case;


Drop abstract constraint
------------------------

:eql-statement:
:eql-haswith:

Remove an abstract constraint from the schema.

.. eql:synopsis::

  [ with [ <module-alias> := ] module <module-name> ]
  drop abstract constraint <name> ;


Description
^^^^^^^^^^^

The command ``drop abstract constraint`` removes an existing abstract
constraint item from the database schema. If any schema items depending
on this constraint exist, the operation is refused.


Parameters
^^^^^^^^^^

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
  An optional list of module alias declarations to be used in the
  migration definition.

:eql:synopsis:`<name>`
  The name (optionally module-qualified) of the constraint to remove.


Example
^^^^^^^

Drop abstract constraint ``upper_case``:

.. code-block:: edgeql

  drop abstract constraint upper_case;


Create constraint
-----------------

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
^^^^^^^^^^^

The command ``create constraint`` defines a new concrete constraint. It can
only be used in the context of :eql:stmt:`create scalar type`,
:eql:stmt:`alter scalar type`, :eql:stmt:`create property`,
:eql:stmt:`alter property`, :eql:stmt:`create link`, or :eql:stmt:`alter link`.

*name* must be a name (optionally module-qualified) of a previously defined
abstract constraint.


Parameters
^^^^^^^^^^

Most sub-commands and options of this command are identical to the
:ref:`SDL constraint declaration <ref_eql_sdl_constraints_syntax>`,
with some additional features listed below:

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
  An optional list of module alias declarations to be used in the
  migration definition.

:eql:synopsis:`set errmessage := <error_message>`
  An optional string literal defining the error message template
  that is raised when the constraint is violated. Other than a
  slight syntactical difference, this is the same as the corresponding
  SDL declaration.

:eql:synopsis:`create annotation <annotation-name> := <value>;`
  An optional list of annotations for the constraint. See
  :eql:stmt:`create annotation` for details.


Example
^^^^^^^

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
----------------

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
^^^^^^^^^^^

The command ``alter constraint`` changes the definition of a concrete
constraint. Both single- and multi-command forms are supported.


Parameters
^^^^^^^^^^

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
  An optional list of module alias declarations for the migration.

:eql:synopsis:`<name>`
  The name (optionally module-qualified) of the concrete constraint
  that is being altered.

:eql:synopsis:`<argspec>`
  A list of constraint arguments as specified at the time of
  ``create constraint``.

:eql:synopsis:`on ( <subject-expr> )`
  An expression defining the *subject* of the constraint as specified
  at the time of ``create constraint``.

The following subcommands are allowed in the ``alter constraint`` block:

:eql:synopsis:`set delegated`
  Mark the constraint as *delegated*, which means it will
  not be enforced on the type it's declared on, and enforcement is
  delegated to subtypes. Useful for :eql:constraint:`exclusive` constraints.

:eql:synopsis:`set not delegated`
  Mark the constraint as *not delegated*, so it is enforced globally across
  the type and any extending types.

:eql:synopsis:`rename to <newname>`
  Change the name of the constraint to ``<newname>``.

:eql:synopsis:`alter annotation <annotation-name>`
  Alter a constraint annotation.

:eql:synopsis:`drop annotation <annotation-name>`
  Remove a constraint annotation.

:eql:synopsis:`reset errmessage`
  Remove the error message from this constraint, reverting to that of the
  abstract constraint, if any.

All subcommands allowed in ``create constraint`` are also valid in
``alter constraint``.

Example
^^^^^^^

Change the error message on the minimum value constraint on the property
"score" of the "User" type:

.. code-block:: edgeql

  alter type User alter property score
    alter constraint min_value(0)
      set errmessage := 'Score cannot be negative';


Drop constraint
---------------

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
^^^^^^^^^^^

The command ``drop constraint`` removes the specified constraint from
its containing schema item.


Parameters
^^^^^^^^^^

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
  Optional module alias declarations for the migration definition.

:eql:synopsis:`<name>`
  The name (optionally module-qualified) of the concrete constraint
  to remove.

:eql:synopsis:`<argspec>`
  A list of constraint arguments as specified at the time of
  ``create constraint``.

:eql:synopsis:`on ( <subject-expr> )`
  Expression defining the *subject* of the constraint as specified
  at the time of ``create constraint``.

Example
^^^^^^^

Remove constraint "min_value" from the property "score" of the "User" type:

.. code-block:: edgeql

  alter type User alter property score
    drop constraint min_value(0);


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Introspection > Constraints <ref_datamodel_introspection_constraints>`
  * - :ref:`Standard Library > Constraints <ref_std_constraints>`
