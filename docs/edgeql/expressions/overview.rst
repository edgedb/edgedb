.. _ref_eql_expr:


Overview
========

:edb-alt-title: Expressions Overview


Expressions are used to represent a *value* or a *set of values* in EdgeQL
commands.


.. _ref_eql_expr_index_literal:

Scalar Literals
---------------

EdgeQL supports the following scalar literals:

====================================== =============================
 Syntax                                 Type
====================================== =============================
 :eql:code:`SELECT true;`               :eql:type:`bool`
 :eql:code:`SELECT false;`              :eql:type:`bool`
 :eql:code:`SELECT 42;`                 :eql:type:`int64`
 :eql:code:`SELECT -1.1;`               :eql:type:`float64`
 :eql:code:`SELECT 1e-3;`               :eql:type:`float64`
 :eql:code:`SELECT -42n;`               :eql:type:`bigint`
 :eql:code:`SELECT 100.1n;`             :eql:type:`decimal`
 :eql:code:`SELECT 1e+100n;`            :eql:type:`decimal`
 :eql:code:`SELECT 'hello';`            :eql:type:`str`
 :eql:code:`SELECT r'hello';`           :eql:type:`str` (raw string)
 :eql:code:`SELECT $$ CREATE .. $$;`    :eql:type:`str` (raw string)
 :eql:code:`SELECT b'bina\\x01ry';`     :eql:type:`bytes`
====================================== =============================

Refer to :ref:`lexical structure <ref_eql_lexical_const>` for more details
about the syntax for standard scalar literals.

Additionally, many scalar values can be represented as
a cast string literal:

.. code-block:: edgeql

    SELECT <int16>'1' = <int16>1;
    SELECT <float32>'1.23';
    SELECT <duration>'1 day';
    SELECT <decimal>'1.23' = 1.23n;


EdgeQL defines many functions and operators to work with various
scalar types, see the :ref:`functions and operators <ref_eql_funcops>`
section for more details.


.. _ref_eql_expr_index_setref:

Set References
--------------

A set reference is an *name* (a simple identifier or a qualified schema name)
that represents a set of values.  It can be the name of an object type, the
name of a view, or an *alias* defined in a statement.

For example, in the following query ``User`` is a set reference:

.. code-block:: edgeql

    SELECT User;

See :ref:`this section <ref_eql_fundamentals_references>` for more
information about set references.


.. _ref_eql_expr_index_path:

Paths
-----

A *path expression* (or simply a *path*) represents a set of values that are
reachable when traversing a given sequence of links or properties from some
source set.  For example, here is s a path that represents the names of all
friends of all ``User`` objects in the database.

.. code-block:: edgeql

    SELECT User.friends.name;

Path expression syntax and semantics are described in detail in a
:ref:`dedicated section <ref_eql_expr_paths>`.


.. _ref_eql_expr_index_shape:

Shapes
------

A *shape* is a powerful syntactic construct that can be used to dynamically
describe a portion of an object graph.  For example, the below query returns
a set of ``Issue`` objects and includes a ``number`` and an associated
owner ``User`` object, which in turn includes the ``name`` and the
``email`` for that user:

.. code-block:: edgeql-repl

    db> SELECT
    ...     Issue {
    ...         number,
    ...         owner: {  # sub-shape, selects Issue.owner objects
    ...            name,
    ...            email
    ...         }
    ...     };

    {
        'number': 1,
        'owner': {
            'name': 'Alice',
            'email': 'alice@example.com'
        }
    }

See :ref:`this section <ref_eql_expr_shapes>` for more information on
shape syntax and semantics.


.. _ref_eql_expr_index_param:

Query Parameters
----------------

A parameter reference is used to indicate a value that is supplied externally
to an EdgeQL expression.  Parameter references are used in parametrized
statements.  The form of a parameter reference is:

.. code-block:: edgeql

    SELECT $name;


.. _ref_eql_expr_index_operator:

Operators
---------

Most operators in EdgeQL are *binary infix* or *unary prefix* operators.
Some operators have dedicated syntax, like the :eql:op:`IF..ELSE` operator.

Binary infix operator syntax:

.. eql:synopsis::

    <expression> <operator> <expression>

Unary prefix operator syntax:

.. eql:synopsis::

    <operator> <expression>

A complete reference of standard EdgeQL operators can be found in
:ref:`ref_eql_funcops`.


.. _ref_eql_expr_index_parens:

Parentheses
-----------

Expressions can be enclosed in parentheses to indicate explicit evaluation
precedence and to group subexpressions visually for better readability:

.. code-block:: edgeql

    SELECT (1 + 1) * 2 / (3 + 8);


.. _ref_eql_expr_index_function_call:

Function Calls
--------------

The syntax for a function call is as follows:

.. eql:synopsis::

    <function-name> "(" [<argument> [, <argument> ...]] ")"

Here :eql:synopsis:`<function_name>` is a possibly qualified name of a
function, and :eql:synopsis:`<argument>` is an *expression* optionally
prefixed with an argument name and the assignment operator (``:=``).

A complete reference of standard EdgeQL functions can be found in
:ref:`ref_eql_funcops`.


.. _ref_eql_expr_index_typecast:

Type Casts
----------

A type cast expression converts the specified value to another value of
the specified type:

.. eql:synopsis::

    "<" <type> ">" <expression>

The :eql:synopsis:`<type>` must be a valid :ref:`type expression
<ref_eql_types>` denoting a non-abstract scalar or a container type.

For example, the following expression casts an integer value into a string:

.. code-block:: edgeql-repl

    db> SELECT <str>10;
    {"10"}

See the :eql:op:`type cast operator <CAST>` section for more
information on type casting rules.


.. _ref_eql_expr_index_set_ctor:

Set Constructor
---------------

A *set constructor* is an expression that consists of a sequence of
comma-separated expressions enclosed in curly braces:

.. eql:synopsis::

    "{" <expr> [, ...] "}"

A set constructor produces the result by appending its elements.  It is
perfectly equivalent to a sequence of :eql:op:`UNION` operators.

An *empty set* can also be created by omitting all elements.
In situations where EdgeDB cannot infer the type of an empty set,
it must be used together with a type cast:

.. code-block:: edgeql-repl

    db> SELECT {};
    EdgeQLError: could not determine the type of empty set

    db> SELECT <int64>{};
    {}


Tuples
------

A *tuple* is collection of values of possibly different types.  For
example:

.. code-block:: edgeql-repl

    db> SELECT (1.0, -2.0, 'red');
    {(1.0, -2.0, 'red')}
    db> SELECT (180, 82);
    {(180, 82)}
    db> SELECT (180, 82).0;
    {180}

EdgeQL also supports *named tuples*:

.. code-block:: edgeql-repl

    db> SELECT (x := 1.0, y := -2.0, color := 'red');
    {(x := 1.0, y := -2.0, color := 'red')}
    db> SELECT (height := 180, weight := 82);
    {(height := 180, weight := 82)}
    db> SELECT (height := 180, weight := 82).height;
    {180}
    db> SELECT (height := 180, weight := 82).1;
    {82}

Tuples can be nested in arrays, returned from functions, be
a valid object property type.

See the :ref:`tuple expression reference <ref_eql_expr_tuple_ctor>`
for more information on tuple constructors and accessing tuple elements.


.. _ref_eql_expr_index_array_ctor:

Arrays
------

An array is a collection of values of the same type.  For example:

.. code-block:: edgeql-repl

    db> SELECT [1, 2, 3];
    {[1, 2, 3]}
    db> SELECT ['hello', 'world'];
    {['hello', 'world']}
    db> SELECT [(1, 2), (100, 200)];
    {[(1, 2), (100, 200)]}

See :ref:`array expression reference <ref_eql_expr_array_ctor>` for more
information on array constructors.


.. _ref_eql_expr_index_stmt:

Statements
----------

Any ``SELECT`` or ``FOR`` statement, and, with some restrictions, ``INSERT``,
``UPDATE`` or ``DELETE`` statements may be used as expressions.  Parentheses
are required around the statement to disambiguate:

.. code-block:: edgeql

    SELECT 1 + (SELECT len(User.name));

See the :ref:`statements <ref_eql_statements>` section for more information.
