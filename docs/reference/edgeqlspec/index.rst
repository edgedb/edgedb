=====================
EdgeQL Specification
=====================

Introduction
------------

[Insert introduction to EdgeQL here]

Types
-----

In EdgeQL, there are several built-in types that can be used to represent different kinds of data. These include:

- ``str``: A string of characters
- ``int``: An integer value
- ``float``: A floating-point number
- ``bool``: A Boolean value (either ``true`` or ``false``)
- ``datetime``: A date and time value
- ``uuid``: A universally unique identifier

Values
------

In EdgeQL, values can be represented using literal syntax. For example, a string value can be represented using quotes, an integer value can be represented using a numeric literal, and a Boolean value can be represented using the keywords ``true`` or ``false``. For example:


Expressions
-----------

In EdgeQL, expressions are used to represent operations and computations that involve values. These can include arithmetic operations, comparison operations, and logical operations. For example:

Type Checking
-------------

In EdgeQL, type checking is performed at compile time to ensure that expressions and operations are used with the correct types of values. For example, adding a string value to an integer value would result in a type error. Type checking can help catch these errors before the code is executed.

Evaluation
----------

In EdgeQL, expressions are evaluated at runtime to produce a result. For example, an arithmetic expression like ``1 + 2`` would be evaluated to produce the value ``3``. Evaluation can involve type coercion, where values are automatically converted to a compatible type before an operation is performed.

Conclusion
----------

[Insert conclusion to EdgeQL specification here]




.. toctree::
    :hidden:



    overview
    select_hoist


ef


Converted 
=====================

Overview
========

Query Processing Phases
-----------------------

The overview of how EdgeQL are executed:

Features Covered and Those Not Covered
--------------------------------------

The following features are covered:

#. |image|\ **EdgeQL**\ selects, inserts, updates

The following features are not covered (or not yet covered):

#. |image|\ **EdgeQL**\ group by, explain, describe

#. default properties

#. rewrites on properties

#. access policies

#. indexes

#. inheritance

#. schema computables

.. _`sec:edgeql_syntax`:

Syntax
======

Define the following syntactic categories.

Type Skeletons
--------------

.. math:: \tau ::= <primitive> \mid  \{l_1 : \tau_1^{m_1}, \dots, l_n : \tau_n^{m_n}\} \mid N \mid \tau_1 \mathop{\operatorname{\mathtt{{@}}}}\tau_2

.. math:: \mid \operatorname{\mathsf{{prod}}}(\tau_1, \dots, \tau_n) \mid \operatorname{\mathsf{{prod}}}(l_1 : \tau_1, \dots, l_n : \tau_n) \mid \operatorname{\mathsf{{arr}}}(\tau)

.. math:: \mid \tau_1 \lor \tau_2 \mid \tau_1 \land \tau_2 \mid any \mid some_\mathbb{N}

Any expression in |image|\ **EdgeQL**\ will compute an expression of
type :math:`\tau^{m}` (:math:`m` is explained below).

In |image|\ **EdgeQL**\ IR, [index:types]\ *types* are primitive types,
function types, object types, link types with link properties, unnamed
tuple types, named tuple types, union types, any types and nat-indexed
some types (can only be used in function typings).

Generic types can only be used in the specification of function types.

:math:`N` represents [index:table names]\ *table names*. :math:`l`
represents [index:primitive label names]\ *primitive label names*.

We require :math:`\tau_2` to be an object type in
:math:`\tau_1 \mathop{\operatorname{\mathtt{{@}}}}\tau_2`. We also
assume that :math:`\tau \mathop{\operatorname{\mathtt{{@}}}}\{\}` (a
type with no link properties) and :math:`\tau` (the type itself) are the
same.

Cardinality and Multiplicity
-----------------------------

Let :math:`i` denote any [index:cardinal number]\ *cardinal number*,
(i.e., natural numbers, :math:`0`, and :math:`+\infty`),

.. math:: c ::= (\le i) \mid (=i) \mid (\ge i) \mid [i_1, i_2] \mid (*)

.. math:: m ::= c, i

The [index:cardinality mode]\ *cardinality mode* :math:`c` is represents
as a range on cardinal numbers :math:`[i_1, i_2]`, with inclusive lower
and upper bounds, and :math:`i_1 \le i_2`. We have the following
syntactic sugars:

#. :math:`(\le i)` stands for :math:`[0, i]`

#. :math:`(= i)` stands for :math:`[i, i]`

#. :math:`(\ge i)` stands for :math:`[i, +\infty]`

#. :math:`(*)` stands for :math:`[0, +\infty]`

The property specification *optional*, *required*, *multi required* and
*multi* correspond to :math:`(\le 1)`, :math:`(=1)`, :math:`(\ge 1)` and
:math:`(*)` respectively. The mode :math:`(*)` is the default mode, and
can be omitted.

The [index:multiplicity]\ *multiplicity* is a cardinal number that
represents the maximum number of times any element can appear inside a
multiset.

The [index:cardinality and multiplicity mode]\ *cardinality and
multiplicity mode* :math:`m` is written :math:`[i_1, i_2], i_3` where
:math:`[i_1, i_2]` is the cardinality mode and :math:`i_3` is the
multiplicity. We may omit the multiplicity :math:`i_3` if it is the same
as :math:`i_2`.

Modes admit algebraic operations of sum and product.

:math:`[i_1, i_2], i_3 \times [i_4, i_5],i_6 = [i_1 \times i_2, i_4 \times i_5], i_3\times i_6`.

:math:`[i_1, i_2], i_3 + [i_4, i_5], i_6 = [i_1 + i_2, i_4 + i_5], i_3 + i_6`.

As a result, we have :math:`(=1) + (*) = (\ge 1)`, as
:math:`(=1) + (*) = [1, 1] + [0, +\infty] = [1, +\infty]`.

We use the meta-level function :math:`lb`, :math:`ub`, :math:`mult` to
extract out the lower bound, upper bound and multiplicity of a mode.
That is, :math:`lb([i_1, i_2], i_3) = i_1`,
:math:`ub([i_1, i_2], i_3) = i_2`, :math:`mult([i_1, i_2], i_3) = i_3`.

Subtyping of Mods
-----------------

We admit the following partial order on cardinality modes,
:math:`m_1 \le m_2` iff :math:`m_2` is a larger range than :math:`m_1`.
That is, :math:`[i_1, i_2], i_5 \le [i_3, i_4],i_6` if and only if
:math:`i_3 \le i_1`, and :math:`i_2 \le i_4`, and :math:`i_5 \le i_6`.

Examples of expressions and their types:

.. math:: 1 : int^{(=1)}

.. math:: \{1\} : int^{(=1)}

.. math:: \{\{1\}\} : int^{(=1)}

.. math:: \{1,2\} : int^{(=2)}

.. math:: \{1,2\} : int^{(\ge 1)}

.. math:: \{1,2\} : int^{(*)}

.. math:: \{1,2\} : int^{(=2), 1}

.. math:: \{1,1,2\} : int^{(=3), 2}

.. math:: \{1,1,2\} : int^{(=3), 3}

Subtyping of Types
------------------

We have

#. :math:`\tau_1 \le \tau_1 \lor \tau_2`

#. :math:`\tau_2 \le \tau_1 \lor \tau_2`

#. :math:`\tau \le any`.

#. :math:`\tau_1\land \tau_2 \le \tau_1`

#. :math:`\tau_1\land \tau_2 \le \tau_2`

We also have [index:shape subtype]\ *shape subtype* (denoted
:math:`\le_s`) and [index:insert subtype]\ *insert subtype* (denoted
:math:`\le_i`), where shape subtype allows addition of keys in an object
type and insert subtype allows dropping of keys in an object type.

Objects may add keys in the subtype to account for computed properties.

In the type checking rules, there is an implicit subtyping of typing
with mods.

Types are structural and covariant for all other types.

Parameter Modifiers
-------------------

.. math:: p ::= 1 \mid ? \mid *

The [index:parameter modifier]\ *parameter modifier* :math:`p` can be
singleton (which enables broadcasting), optional, or set of. The
singleton parameter indicates this argument will be broadcasted, and the
result cardinality may change accordingly. A function :math:`f`
(primitive or not) will have type
:math:`[\tau_1^{p_1}, \dots, \tau_n^{p_n}] \to \tau^{m}`, and will be
written as the categorical judgment

.. math:: f : [\tau_1^{p_1}, \dots, \tau_n^{p_n}] \to \tau^{m}

Appearances of generic types :math:`some_i`\ ’s with the same index
:math:`i`\ ’s will be constrained to be equal. As seen below in the
coalescing operators.

Some built-in functions:

.. math:: + : [int^{1}, int^{1}] \to int^{(=1)}

.. math:: = \ : [some_0^{1}, some_0^{1}] \to bool^{(=1)}

.. math:: count : [some_0^{*}] \to int^{(=1)}

.. math:: ?? : [some_0^{?}, some_0^{*}] \to some_0^{(*)}

(also OK: :math:`count : [any^{*}] \to int^{(=1)}`)

The parameter modifier may *match* modes and may produce *broadcasting
factor* as a result according to the following rules:

#. :math:`1` matches any :math:`[i_1, i_2], i_3` with broadcasting
   factor :math:`[i_1, i_2],i_3`

#. :math:`?` matches any :math:`[i_1, i_2], i_3` with broadcasting
   factor :math:`[max(1, i_1), max(1, i_2)],  max(1,i_3)`

#. :math:`*` matches any :math:`[i_1, i_2], i_3` with broadcasting
   factor :math:`(=1)`

We use the judgment :math:`p \rhd m \rightsquigarrow m'` to mean that
the parameter modifier :math:`p` matches mode :math:`m` and produces
broadcasting factor :math:`m'`.

When a function
:math:`f : [\tau_1^{p_1}, \dots, \tau_n^{p_n}] \to \tau^{m}` is applied
to expressions of type :math:`\tau_1^{m_1}, \dots, \tau_n^{m_n}`, if
each :math:`p_i` matches :math:`m_i` with broadcasting factor
:math:`m_i'`, then the result of the function application will be an
expression of :math:`\tau^{m_1' \times \dots \times m_n' \times m}`.

For instance,

.. math:: 1 + 1 : int^{(=1)}

.. math:: 1 + \{2,3\} : int^{(=2)}

.. math:: 1 + \{2,3\} : int^{(\ge 2)}

.. math:: 1 + \{2,3\} : int^{(*)}

Primitive Types
---------------

We have the following [index:primitive types]\ *primitive types*:

.. math:: <primitive> ::= int \mid bool \mid str  \mid int_\infty

.. math:: \mid datetime \mid json \mid uuid

Only :math:`int`, :math:`bool`, and :math:`str` have value expressions,
values of other primitive types are obtainable through type casts

:math:`int_\infty` is reserved for internal use and is the set of
natural numbers with infinity.

Expressions
-----------

.. math:: L ::= l \mid @l

A [index:label]\ *label* :math:`L` is either a primitive label or a link
property label.

.. math::

   e ::= v \mid V \mid U \mid  <\tau>(e) \mid \{e_1, \dots , e_n\}
   % \mid (e_1, \dots, e_n) 
   \mid \{L_1 := e_1, \dots, L_n := e_n\}

\ 

.. math::

   \mid e\, s \mid e_1 \cup e_2 \mid
   f(e_1, \dots, e_n)  \mid x \mid e \cdot l \mid \operatorname{\mathsf{{with}}}(x := e_1; e_2)\mid  N \mid e \mathop{\operatorname{\mathtt{{@}}}}l
   \mid \mathop{\operatorname{\mathit{{detached}}}}\, e

.. math:: \mid e \mathop{\operatorname{\mathtt{\cdot_{\leftarrow}}}}l \mid e \ {[is\, {N}]} \mid \mathop{\operatorname{\mathit{{select}}}}\, e

.. math::

   \mid e_1\, \mathop{\operatorname{\mathit{{filter}}}}\, (x.e_2)\,\mathop{\operatorname{\mathit{{order}}}}\, (x.e_3) \mid 
       e_1\, \mathop{\operatorname{\mathit{{offset}}}}\, e_2 \, \mathop{\operatorname{\mathit{{limit}}}}\, e_3
   \mid insert\, N\, s \mid update \, e\, s \mid delete\, e

.. math:: \mid \operatorname{\mathsf{{for}}}(x \leftarrow e_1;  e_2) \mid \operatorname{\mathsf{{optional\_for}}}(x \leftarrow e_1; e_2) \mid e_1 \mathop{\operatorname{\mathit{{if}}}} e_2 \mathop{\operatorname{\mathit{{else}}}} e_3

.. math:: \mid (e_1, \dots, e_n) \mid (l_1 := e_1, \dots, l_n := e_n) \mid [e_1, \dots, e_n]

The [index:expressions]\ *expressions* :math:`e` are primitive values,
computed values (cannot be written down directly but appears as part of
the execution), computed value sets, type casts, multi sets, objects
with link properties

expressions with shapes (defined later), unions, function calls,
variables, object projection (we use :math:`\cdot` instead of ``.`` to
distinguish projections from binders), let-in bindings, type names, link
property projections, detached expressions

backlinks, type intersections, selects (explicit scope operation),

selects with filter and order, select with offset and limit, inserts,
updates, deletes

for loops (:math:`x` is bound in :math:`e_2`), optional for loops,
if-else expressions,

unnamed tuples and named tuples, arrays.

Property Markers
----------------

.. math:: u ::= \mathop{\operatorname{\mathit{{visible}}}} \mid \mathop{\operatorname{\mathit{{invisible}}}}

A [index:property marker]\ *property marker* :math:`u` marks which
properties are visible and invisible when printed to user or returned as
a result. Shapes may change the property marker.

Union of Object Components
--------------------------

The operation :math:`\uplus` denotes the union of two object (i.e. sets
of object components) defined as follows, where the properties of the
right-hand side operand takes priority.

.. math::

   \{L_1 := e_1, \dots, L_n : = e_n\}\uplus
       \{L_1' := e_1', \dots, L_k' : = e_k'\}

.. math::

   = 
       \{(L_i := e_i \mid  L_i \notin \{L_j'\}_{1 \le j \le k } )_{1 \le i \le n}, 
      L_1' := e_1', \dots, L_k' : = e_k'\}

Values
------

.. math:: v ::= 3 \mid 3.14 \mid \mathop{\operatorname{\mathit{{true}}}}\mid \mathop{\operatorname{\mathit{{false}}}}\mid \operatorname{\mathsf{{``<string>"}}} \mid \infty

.. math:: \mid datetime(D) \mid json(D)

The [index:primitive values]\ *primitive values* :math:`v` include
integers, floating point numbers, string literals, and boolean literals.
The variable :math:`D` represents
[index:implementation-dependent data]\ *implementation-dependent data*
for representing datatypes such as datetime and json.

The value :math:`\infty` is reserved for internal use and is the
infinity value of type :math:`int_\infty`.

Primitive values have categorical types, written :math:`v : \tau`. For
example, we have :math:`3 : int`, :math:`json(D) : json`, and
:math:`hello : str`.

Let :math:`id` denote a globally unique identifier (our real
implementation uses UUIDs).

.. math:: U ::= \{ V_1, \dots, V_n\}

.. math::

   V ::=   v\mid  \operatorname{\mathsf{{ref}}}(id):W
              \mid \operatorname{\mathsf{{free}}} : W
              \mid \operatorname{\mathsf{{link}}}(id_s, l, id_t) \mathop{\operatorname{\mathtt{{@}}}}W_2

.. math:: \mid (V_1, \dots, V_n) \mid (l_1 := V_1, \dots, l_n := V_n) \mid [V_1, \dots, V_n]

.. math:: W ::= \{L_1^{u_1} := U_1, \dots, L_n^{u_n} := U_n\}

A [index:set of values]\ *set of values* :math:`U` is an ordered
multi-set of values and admits the operation of union (on multisets) in
the obvious way.

:math:`V` : [index:values]\ *values* include primitive values, object
references with possibly computed properties, free objects with possibly
computed properties, links with properties unnamed tuples and named
tuples, arrays.

Note to links with properties:
:math:`\operatorname{\mathsf{{link}}}(id_1, l, id_2) \mathop{\operatorname{\mathtt{{@}}}}W_2`
must be of the form
:math:`\operatorname{\mathsf{{link}}}(id_1, l, id_2) \mathop{\operatorname{\mathtt{{@}}}}\{l_1 := V_1, \dots, l_n := V_n\}`.
We use the operation
:math:`\mathop{\operatorname{\mathit{{get\_link\_target}}}}` to denote
the operation of getting the target. For example,
:math:`\mathop{\operatorname{\mathit{{get\_link\_target}}}}(\operatorname{\mathsf{{link}}}(2,l, 1) \mathop{\operatorname{\mathtt{{@}}}}W_1) = \operatorname{\mathsf{{ref}}}(1) : \{\}`.
We also have an operation on sets of possible link property values that
will retrieve the link target and perform deduplication on a set of link
with properties, and will leave value sets that are not links unchanged.
For example,
:math:`\mathop{\operatorname{\mathit{{assume\_link\_target}}}}(\{\operatorname{\mathsf{{link}}}(2, l_1, 1) \mathop{\operatorname{\mathtt{{@}}}}W_1, \operatorname{\mathsf{{link}}}(3, l_2, 1) \mathop{\operatorname{\mathtt{{@}}}}W_2\}) = \operatorname{\mathsf{{ref}}}(1) : \{\}`

:math:`W` : [index:objects]\ *objects* serve as value objects without
:math:`\operatorname{\mathsf{{free}}}` or
:math:`\operatorname{\mathsf{{ref}}}` classifications.

We may substitute any of the :math:`U, V, W` for a variable in an
expression and the resulting expression will still be formed. Multiset
constructions of :math:`U` get coerced to multiset constructions of
expressions.

The operation :math:`\uplus` extends naturally to operate on object
components of :math:`W`.

Meta-level Operations on Objects
------------------------------------

Let
:math:`\mathop{\operatorname{\mathit{{remove\_unless\_link\_prop}}}}` be
the meta-level function that removes all top level properties from an
expression, a value or an object unless it is a link property. TODO:
check and disgard

Let :math:`\mathop{\operatorname{\mathit{{obj\_to\_link\_prop\_obj}}}}`
be the meta-level function that converts a plain object free of link
properties to an object with only link properties, and let
:math:`\mathop{\operatorname{\mathit{{link\_prop\_obj\_to\_obj}}}}` be
the meta-level function that does the reverse conversion. That is,

.. math::

   \{ l_1 ^{u_1} := U_1, \dots, l_n ^{u_n} := U_n \}
    \overset{\mathop{\operatorname{\mathit{{obj\_to\_link\_prop\_obj}}}}}{\underset{\mathop{\operatorname{\mathit{{link\_prop\_obj\_to\_obj}}}}}{\rightleftharpoons}} 
   \{ @l_1 ^{u_1} := U_1, \dots, @l_n ^{u_n} := U_n \}

Example of Link Properties and Their IR Representations
-----------------------------------------------------------

Link properties are attached to objects.

In the schema:

::

       type Person {
           required property name -> str ;
           multi link friends -> Person {
               property since -> str
           }
       }

A query ``select Person {id, name, friends : { id, name, @since }};``
will return the following (for example):

::

   {
     default::Person {
       id: 01,
       name: `p3',
       friends: {
           default::Person {id: e0, name: `p1', @since: {}}, 
           default::Person {id: e5, name: `p2', @since: `t6'}
           }
       }
   }

And the result is the following value in IR:

.. math::

   \begin{aligned}
   \operatorname{\mathsf{{ref}}}(01):\{& \\
       name := & \{p3\}, \\
       friends :=  &\{\\
               &\operatorname{\mathsf{{ref}}}(e0) : \{ name :=  \{p1\},
                @since := \{\} \},\\
               &\operatorname{\mathsf{{ref}}}(e5) : \{ name :=  \{p2\},
               @since := \{t6\} \}\\
          &\} \\
       \}& \\\end{aligned}

TODO: figure out which :math:`id` stores link properties

Shape
-----

A [index:shape]\ *shape* :math:`s` specifies how an object should be
printed. And is inductively generated by the following grammar, as an
ordered list of [index:shape components]\ *shape components* :math:`c`.
(In theory, we could allow link links, just change the :math:`l : s` to
:math:`L : s`.)

.. math:: s ::=  \{ c_1, \dots , c_n\}

.. math::

   c ::=  L \mid l : s \mid L := x.e
   %    \mid @l \mid @l := x.e

The binder :math:`x` in :math:`x.e` refers to the current object. It
will be utilized in the leading dot notation. In the query
``select X { y := .1 }``, an implicit binder is put before ``.1``. That
is, the expression elaborates into :math:`X\, \{ y := x. (x\cdot 1)\}`.
In this way, the other objects of shape components are just syntax
sugars.

#. :math:`L` where :math:`L = l` is really :math:`n := x. (x \cdot l)`

#. :math:`L` where :math:`L = @l` is really
   :math:`n := x. (x \mathop{\operatorname{\mathtt{{@}}}}l)`

#. :math:`l : s` is really :math:`n := x. ((x \cdot l)\, \{s\})`

:math:`label(c)` denote the label component of :math:`c`.

#. :math:`label(L) = L`

#. :math:`label(l : s) = l`

#. :math:`label(L := x.e) = L`

Shape Object Conversion
-----------------------

We say that a shape is [index:free of actual bindings]\ *free of actual
bindings* if for all components :math:`L := x.e` of the shape, :math:`x`
does not appear in :math:`e`. As a consequence of syntax sugar, if a
shape has shape components of the form :math:`L` or :math:`l : s`, then
it is *not* free of actual bindings. A shape free of actual bindings
looks just like an object. So we have the following object shape
conversion:

:math:`shape\_to\_obj(s)` and :math:`obj\_to\_shape(s)` defined in the
obvious way, and we have the following equivalence (meaning both sides
evaluates to the same result):

.. math:: shape\_to\_obj(s) = \{\}\, s

.. math:: e = \{\}\, (obj\_to\_shape(e))

Note that in both objects and shapes, labels cannot be duplicated.

Schemas Types
-------------

Types :math:`\tau` classify runtime values and expressions. Schema types
:math:`T` and schema type components :math:`t`, on the other hand,
incorporate other elements that are present in a database schema, such
as computed properties.

They are defined as follows:

.. math:: T ::= \{l_1 : M_1^{m_1}, \dots, l_n : M_n^{m_n}\}

.. math:: t ::= <primitive>   \mid N

.. math:: \mid \operatorname{\mathsf{{prod}}}(t_1, \dots, t_n) \mid \operatorname{\mathsf{{prod}}}(l_1 : t_1, \dots, l_n : t_n) \mid \operatorname{\mathsf{{arr}}}(t)

.. math:: \mid t_1 \lor t_2 \mid t_1 \land t_2 \mid T

.. math:: M ::= t \mid t \mathop{\operatorname{\mathtt{{@}}}}T \mid \operatorname{\mathsf{{comp}}}(x. e : t) \mid \operatorname{\mathsf{{comp}}}(x. e: t \mathop{\operatorname{\mathtt{{@}}}}T)

.. math:: \mid \operatorname{\mathsf{{default}}}(x. e : t)  \mid  \operatorname{\mathsf{{default}}} (x.e : t \mathop{\operatorname{\mathtt{{@}}}}T)

The [index:schema types]\ *schema types* :math:`T` map string labels to
type components.

The [index:schema type components]\ *schema type components* :math:`t`
include primitives, type names, labeled and unlabeled products, arrays,
unions and intersections, and free object types

The [index:modified schema type component]\ *modified schema type
component* :math:`M` include schema type components, schema computables,
properties with defaults, both with and without link properties.

We say :math:`t \mathop{\operatorname{\mathtt{{@}}}}T` instead of
:math:`N \mathop{\operatorname{\mathtt{{@}}}}T` because :math:`t` may
contain defaults and computables.

For every modified schema type :math:`M` or schema type components
:math:`t`, there is a corresponding runtime type, denoted
:math:`\mathop{\operatorname{\mathit{{rt\_type}}}}(M)` or
:math:`\mathop{\operatorname{\mathit{{rt\_type}}}}(t)`. With essential
rules as follows:

#. :math:`\mathop{\operatorname{\mathit{{rt\_type}}}}(t \mathop{\operatorname{\mathtt{{@}}}}T) = \mathop{\operatorname{\mathit{{rt\_type}}}}(t) \mathop{\operatorname{\mathtt{{@}}}}\mathop{\operatorname{\mathit{{rt\_type}}}}(T)`

#. :math:`\mathop{\operatorname{\mathit{{rt\_type}}}}(\operatorname{\mathsf{{comp}}}(x.e : t)) = \mathop{\operatorname{\mathit{{rt\_type}}}}(t)`

#. :math:`\mathop{\operatorname{\mathit{{rt\_type}}}}(\operatorname{\mathsf{{default}}}(x.e : t)) = \mathop{\operatorname{\mathit{{rt\_type}}}}(t)`

Of course, the cardinality and multiplicity mode in :math:`t^m` still
plays the role of distinguishing between different kinds of modifiers.

For inheritance, the declarations from the parent object are copied to
the child objects. The subtyping relation is only nominal. TODO: check
effect for speed, change to appeal directly if copying is costly

.. _`sec:statics`:

Statics
=======

Static Contexts
---------------

.. math:: \Delta ::= \cdot \mid \Delta, N := T \mid \Delta, N_1 \le N_2

.. math:: \Gamma ::= \cdot \mid \Gamma, x : \tau^{m}

Static contexts contain two parts, schema definitions, and typing for
variables.

The typing for schemas records both type definitions and subtyping
relations.

It is also crucial that we never have types of the form
:math:`(\tau \mathop{\operatorname{\mathtt{{@}}}}\tau') \mathop{\operatorname{\mathtt{{@}}}}\tau''`,
or
:math:`\tau \mathop{\operatorname{\mathtt{{@}}}}(\tau' \mathop{\operatorname{\mathtt{{@}}}}\tau'')`.

The subtyping judgment :math:`\Delta \vdash N_1 \le N_2` is defined
inductively:

#. :math:`\dots, N_1 \le N_2, \dots  \vdash N_1 \le N_2`

#. :math:`\Delta \vdash N_1 \le N_1`

#. :math:`\Delta \vdash N_1 \le N_3` if
   :math:`\Delta \vdash N_1 \le N_2` and
   :math:`\Delta \vdash N_2 \le N_3`

Type Equivalences
-----------------

We allow :math:`N`, :math:`N \mathop{\operatorname{\mathtt{{@}}}}\{\}`,
:math:`\tau\mathop{\operatorname{\mathtt{{@}}}}\{\}`, :math:`\tau` to be
used interchangeably.

Moreover, we have the following equivalences

#. :math:`\tau_1 \land \tau_2 = \tau_1` if :math:`\tau_2 \le \tau_1`

#. :math:`\tau_1 \lor \tau_2 = \tau_1` if :math:`\tau_2 \le \tau_1`

Effectful vs. Effect-free expressions
-------------------------------------

An expression is an [index:effect-free expressions]\ *effect-free
expressions* if neither :math:`update` nor :math:`insert` appears in it.
We use the judgment
:math:`\mathop{\operatorname{\mathit{{effect\_free}}}}(e)` to denote
that expression :math:`e` is effect-free.

We also overload the predicate
:math:`\mathop{\operatorname{\mathit{{effect\_free}}}}` to act on
function names, so that if a function is effect free, all its arguments
must be effect free. An example effect free function is ``std::IF``.

Dynamic Contexts
----------------

A [index:database storage]\ *database storage* :math:`\mu` is an
unordered tuple mapping UUIDs to their types and data.

.. math:: \mu ::= \{(id_1, N_1,  W_1), \dots, (id_n, N_n,  W_n)\}

Since we need to access static (schema-related) information at runtime,
dynamic contexts are represented as :math:`\mu_\Delta`. Since the schema
relation remains unchanged through the code execution, we may omit the
subscript annotation :math:`_\Delta` and just write :math:`\mu`.

Storage Conventions
-------------------

In a database storage :math:`\mu`, for every entry :math:`(id, N, W)`,
:math:`W` must be of the form
:math:`\{l_1^{\ visible} := U_1, \dots, l_n^{\ visible} := U_n\}`, where
each element of :math:`U_i` is either :math:`v` in case :math:`l_i` is a
property, or
:math:`\operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}\{l_1'^{\ visible} := v_1', \dots, l_n'^{\ visible} := v_n'\}`
in case :math:`l_i` is a link. A link without link properties will
nevertheless be
:math:`\operatorname{\mathsf{{ref}}}(id)\mathop{\operatorname{\mathtt{{@}}}}\{\}`.

Essentially, we do not store redundant information, and all properties
are visible. (Alternatively, we could drop annotation but I prefer less
syntactic constructions)

Context Formation Rules
-----------------------

A context :math:`\Delta; \Gamma` is wellformed if for all :math:`N := T`
in :math:`\Delta`,
:math:`\Delta; \Gamma \vdash^N T \operatorname{\mathsf{{valid}}}`. The
judgment
:math:`\Delta; \Gamma \vdash^{\tau} T \operatorname{\mathsf{{valid}}}`
and
:math:`\Delta; \Gamma \vdash^{\tau} M^{m} \operatorname{\mathsf{{valid}}}`
are defined as follows:

.. container:: mathpar

   ; ^(x.e : t )^m

   ; ^(x.e : t T)^m

   ; ^(x.e : M)^m

   ; ^(t T)^m

   ; ^t ^m

and we have structural rule in other cases. TODO: Elaborate maybe on
link props

Typing Rules for Expressions
----------------------------

The judgment :math:`\Delta; \Gamma \vdash_\mu e : \tau^m` says that
expression :math:`e` computes a value set :math:`U` of type :math:`\tau`
and cardinality and multiplicity :math:`m`, with a dynamic context
:math:`\mu`. We will drop :math:`\mu` if it is not referenced.

The typing rules for expressions:

#. Subtyping of expressions:

   .. math::

      \inferrule{
              \Delta; \Gamma \vdash e : \tau_1^m \\
              \Delta \vdash \tau_1 \le \tau_2 \\
              m \le m'
           }{
              \Delta; \Gamma \vdash e : \tau_2^{m'}
          }

   The subtyping rule needs :math:`\Delta` because we have type
   variables.

#. Primitive values :math:`e = v`

   .. math:: \inferrule{v : \tau}{\Delta; \Gamma \vdash v : \tau^{(=1)}}

#. Values :math:`e = V`

   .. math:: \inferrule{\Delta \vdash _{\mu} V : \tau}{\Delta; \Gamma \vdash_\mu V : \tau^{(=1)}}

#. Value sets :math:`e = U`

   .. math:: \inferrule{\Delta \vdash _{\mu} U : \tau^m}{\Delta; \Gamma \vdash_\mu U : \tau^{m}}

#. Variables

   .. math:: \inferrule{ }{\Delta; \Gamma , x : \tau^m \vdash x : \tau^m}

#. Type casts

   .. math:: \inferrule{\Delta; \Gamma \vdash e : \tau_2^m \\ \tau_2^m \Rightarrow \tau^{m'} }{\Delta; \Gamma \vdash<\tau>(e) : \tau^{m'}}

   Type cast feasibility judgment :math:`\tau_2^m \Rightarrow \tau^{m'}`
   holds iff the type cast can be performed. In general, :math:`m'` is
   equal to :math:`m`. :math:`\tau_2^m` should be primitive here.

#. Expressions with Shapes

   .. math::

      \inferrule{
                  \Delta; \Gamma \vdash e : \tau^m
                  \\
                  \Delta; \Gamma \vdash s : \tau\Rightarrow \tau'
                  \\
                  \mathop{\operatorname{\mathit{{effect\_free}}}}(s)
              }{
                  \Delta; \Gamma \vdash e\, s : (\tau')^m
              }

   The typing for shapes look like follows. Essentially we keep
   everything else but add the computed properties.

   .. math::

      \inferrule{
                  }{
                  \Delta; \Gamma \vdash
                  \{c_1, \dots, c_k\} : 
                  \tau
                  \Rightarrow \tau\\'
                  }

   where :math:` `
   .

   We have :math:`\tau'` =
   :math:`\tau'' \mathop{\operatorname{\mathtt{{@}}}}\tau'''`,

   where :math:`\tau''` is

   .. math::

      \cup_{j, 1\le j\le k}
                  \{\begin{cases}
                     l_i : (\tau_i')^{m_i} & \text{if } c_j = l_i := x.e \land \Delta; \Gamma , x : \tau^{(=1)} \vdash e : (\tau_i')^{m_i}\land \tau_i' \le_{s} \tau_i\\
                     l : \tau^{m} & \text{if } c_j = l := x.e  \land l\ne l_i \land \Delta; \Gamma , x : \tau ^{(=1)}\vdash  e : \tau^{m}\\
                     \text{(derived) } l_i : \tau_i^{m_i} & \text{if } c_j = l_i\\
                     \text{(derived) } l_i : (\tau_i')^{m_i} & \text{if } c_j = l_i : s  \land \Delta; \Gamma \vdash s : \tau_i \Rightarrow \tau'_i\\
                  \end{cases}\}

   .. math::

      \cup
                  (\cup_{i, 1\le i \le n} \{l_i :\tau_i^{m_i}\mid \forall j. l_i \not= label(c_j)\})

   and :math:`\tau'''` is

   .. math::

      \cup_{j, 1\le j \le k}
              \{\begin{cases}
                 l_i : (\tau_i')^{m_i} & \text{if } c_j = @l_i := x.e \land \Delta; \Gamma , x : \tau ^{(=1)}\vdash e : (\tau_i')^{m_i} \land \tau_i' \le_s \tau_i\\
                 l : \tau^{m} & \text{if } c_j = @l := x.e  \land l\ne l_i \land \Delta; \Gamma , x : \tau ^{(=1)}\vdash  e : \tau^{m}\\
                 \text{(derived) } l_i : \tau_i^{m_i} & \text{if } c_j = @l_i\\
              \end{cases}\}

   .. math::

      \cup
              (\cup_{i, 1\le i \le p} \{@l_i :\tau_i^{m_i}\mid \forall j. @l_i \not= label(c_j)\})

   Note that this conversion fails when :math:`c_j = l : s` where
   :math:`\forall i. l \ne l_i`, and when :math:`c_j= l_i := x.e` and
   :math:`e` is not of the correct type. Properties not mentioned in the
   shape are kept instead of discarded.

#. Unions

   .. math:: \inferrule{\Delta; \Gamma \vdash e_1 : \tau_1^m \\ \Delta; \Gamma \vdash e_2 : \tau_2^{m'}  }{\Delta; \Gamma \vdash e_1 \cup e_2 : (\tau_1 \lor \tau_2)^{m + m'}}

#. Function Calls

   .. math::

      \inferrule{f : [\tau_1^{p_1}, \dots, \tau_n^{p_n}] \to \tau^m\\ 
          \forall 1 \le i \le n. (\Delta; \Gamma \vdash e_i : \tau_i^{m_i} \land p_i \rhd m_i \rightsquigarrow m_i') 
          \\
          \mathop{\operatorname{\mathit{{effect\_free}}}}(f) \Rightarrow \forall 1 \le i \le n. \mathop{\operatorname{\mathit{{effect\_free}}}}(e_i)
          }
          {\Delta; \Gamma \vdash f(e_1, \dots,  e_n) : \tau^{m_1' \times \dots m_n' \times m}}

   Some functions like :math:`??` may have special cardinality
   inference. The special cardinality inference is reserved for the
   standard library and is not available for user defined functions.

   (Note : alternative elaboration-style checking:

   .. math::

      \inferrule{f : [\tau_1^{p_1}, \dots, \tau_n^{p_n}] \to \tau^m\\ 
          \forall 1 \le i \le n. (\Delta; \Gamma \vdash e_i : \tau_i^{m_i} \land p_i \rhd m_i \rightsquigarrow m_i') 
          }
          {\Delta; \Gamma \vdash f(e_1, \dots,  e_n) : \tau^{m_1' \times \dots m_n' \times m}
          \rightsquigarrow 
          (\circ{\begin{cases}
              \operatorname{\mathsf{{for}}}(x_i \leftarrow e_i; -) , \text{if}\ p_i = 1  \\
              \operatorname{\mathsf{{optional\_for}}}(x_i \leftarrow e_i; -) ,\\\hspace{6em} \text{if}\ p_i = ?  \\
              % \m{with}(k := \lambda \_. -; \\
              % \hspace{1em}\m{if}(exists(e_i) ;  \\
              %     \hspace{2em}\m{for}(x_i \leftarrow e_i; k()))); \\
              %     \hspace{2em}\m{with}(x_i := e_i; k()),
              %     \text{if}\ p_i = ?  \\
              \operatorname{\mathsf{{with}}}(x_i := e_i; -) , \text{if}\ p_i = *  
          \end{cases}}) [f(x_1, \dots, x_n)]
          }

   where :math:`\circ` is the hole-filling composition, and
   :math:`\rightsquigarrow` denotes elaboration)

#. Object Construction

   .. math::

      \inferrule{ \forall 1 \le i \le k. (
              {\begin{cases}
                  l = l_j \land \Delta; \Gamma \vdash e_i : \tau_j^{m_j}   & \text{if $L_i = l$} \\
                  l = l_j' \land \Delta; \Gamma \vdash e_i : \sigma_j^{m_j'}  &  \text{if $L_i = @l$} \\
              \end{cases}}
                  )}{\Delta; \Gamma \vdash\{L_1 := e_1, \dots, L_k := e_k\} : 
          (\{l_1 : \tau_1^{m_1}, \dots, l_n :\tau_n^{m_n}\}
          \mathop{\operatorname{\mathtt{{@}}}}\{l_1' : \sigma^{m_1'}, \dots, l_p' : \sigma^{m_p'}\})^{(=1)}}

   Objects may leave out properties.

#. Object Projection (to ids objects)

   .. math::

      \inferrule{ \Delta; \Gamma \vdash e : \tau^{m}}
          {\Delta; \Gamma \vdash(e \cdot \operatorname{\mathsf{{id}}}) :\mathop{\operatorname{\mathit{{uuid^{m}}}}}}

   :math:`\operatorname{\mathsf{{id}}}` is a reserved label name.

#. Object Projection (on objects)

   .. math::

      \inferrule{ \Delta; \Gamma \vdash e : (\{l_1 : \tau_1^{m_1}, \dots, l_n :\tau_n^{m_n}\}
                  \mathop{\operatorname{\mathtt{{@}}}}\sigma
          )^{m}}
          {\Delta; \Gamma \vdash(e \cdot l_i) :\tau_i^{m'}}

   where

   .. math::

      {m' = \begin{cases}
              m \times m_i & \text{if $\tau_i$ is primitive}\\
              [min(1, lb(m \times m_i)), ub(m \times m_i)], min(1, lb(m \times m_i)) & \text{if $\tau_i$ is not primitive}\\
          \end{cases}}\ 

   The reason that the cardinality shrinks is that deduplication may be
   performed when retrieving the first component of links with
   propreties.

#. Object Projection (on tuples)

   .. math::

      \inferrule{ \Delta; \Gamma \vdash e : \operatorname{\mathsf{{prod}}}(l_1 : \tau_1, \dots, l_n :\tau_n)^m }
          {\Delta; \Gamma \vdash(e \cdot l_i) :\tau^{m'}}

   .. math::

      \inferrule{ 
              \Delta; \Gamma \vdash e : \operatorname{\mathsf{{prod}}}(l_1 : \tau_1, \dots, l_n :\tau_n)^m 
              \\ \text{ or }
              \Delta; \Gamma \vdash e : \operatorname{\mathsf{{prod}}}(\tau_1, \dots, \tau_n)^m \\ 1 \le i \le n
              }
          {\Delta; \Gamma \vdash(e \cdot i) :\tau^{m'}}

   where

   .. math::

      {m' = \begin{cases}
              m  & \text{if $\tau_i$ is primitive}\\
              [min(1, lb(m )), ub(m)], min(1, lb(m)) & \text{if $\tau_i$ is not primitive}\\
          \end{cases}}\ 

#. Links Property Projections

   .. math::

      \inferrule{\Delta; \Gamma \vdash e_1 : (N\mathop{\operatorname{\mathtt{{@}}}}\{l_1 : \tau_1^{m_1}, \dots, l_n :\tau_n^{m_n}\})^{m}  }
          {\Delta; \Gamma \vdash  e_1 \mathop{\operatorname{\mathtt{{@}}}}l_i :  (\tau_i)^{m \times m_i}}

   We further require that :math:`\tau_i` is primitive.

   Restricting ourselves to only link properties seems arbitrary. We
   could have link links and link link link links.

#. Backlinks

   .. math::

      \inferrule{\Delta; \Gamma \vdash e : \tau^m}
          {\Delta; \Gamma \vdash  e \mathop{\operatorname{\mathtt{\cdot_{\leftarrow}}}}l :  (\lor_i \{N_i \mathop{\operatorname{\mathtt{{@}}}}\tau'' \mid  \mid N_i := \{\dots, l : \tau'\mathop{\operatorname{\mathtt{{@}}}}\tau'', \dots\} \in \Delta\})^{(*)}}

   TODO: link properties of back links,
   see\ https://edgedb.slack.com/archives/C04JG7CR04T/p1677116775282399

#. Type Intersections

   .. math::

      \inferrule{\Delta; \Gamma \vdash e : \tau^m \\ \text{$\tau$ is not a union of link types}}
          {\Delta; \Gamma \vdash  e \ {[is\, {N}]} :  (\tau\land N) ^{[0, ub(m)], mult(m)}}

   .. math::

      \inferrule{\Delta; \Gamma \vdash e : (\lor_i(N_i\mathop{\operatorname{\mathtt{{@}}}}\tau_i'))^m}
          {\Delta; \Gamma \vdash  e \ {[is\, {N}]} :  (\lor_j\{N_j\mathop{\operatorname{\mathtt{{@}}}}\tau_j' \mid N_j \le N\}) ^{[0, ub(m)], mult(m)}}

#. Selects

   .. math::

      \inferrule{ \Delta; \Gamma \vdash e : \tau^m}
          {\Delta; \Gamma \vdash\mathop{\operatorname{\mathit{{select}}}}\, e : \tau^{m}}

#. Detached expressions

   .. math::

      \inferrule{ \Delta; \Gamma \vdash e : \tau^m}
          {\Delta; \Gamma \vdash\mathop{\operatorname{\mathit{{detached}}}}\, e : \tau^{m}}

#. Let in bindings

   .. math::

      \inferrule{\Delta; \Gamma \vdash e_1 : \tau_2^{m_2} \\ \Delta; \Gamma , x : \tau_2^{m_2} \vdash e_2 : \tau^m}
          {\Delta; \Gamma \vdash\operatorname{\mathsf{{with}}}(x := e_1; e_2) : \tau^m}

#. Type Names

   .. math:: \inferrule{  }{\Delta, N := T; \Gamma \vdash  N :  \mathop{\operatorname{\mathit{{rt\_type}}}}(T)^{(*)}}

#. Selects with filter and order

   .. math::

      \inferrule{
              \Delta; \Gamma \vdash e_1 : \tau^{m} \\
              \Delta; \Gamma , x : \tau^{(=1)} \vdash e_2 : bool^{m'} \\
              \Delta; \Gamma , x : \tau^{(=1)} \vdash e_3 : \{l_1 : \tau_1^{(\le 1)}, \dots, l_n : \tau_n^{(\le 1)}\}^{(=1)} \\
              \mathop{\operatorname{\mathit{{effect\_free}}}}(e_2) \\
              \mathop{\operatorname{\mathit{{effect\_free}}}}(e_3) \\
              \mathop{\operatorname{\mathit{{order\_spec}}}}(l_1, \dots, l_n)
          }{
              \Delta; \Gamma \vdash e_1\,\mathop{\operatorname{\mathit{{filter}}}}\, (x.e_2)\,\mathop{\operatorname{\mathit{{order}}}}\, (x.e_3) : \tau^{[0, ub(m)], mult(m)}
          }

   The predicate
   :math:`\mathop{\operatorname{\mathit{{order\_spec}}}}(l_1, \dots, l_n)`
   says that :math:`l_1` through :math:`l_n` forms an
   [index:order specification list]\ *order specification list*.
   Essentially, the keys uniquely specifies (1) which order to execute
   first and last (2) for each order, whether it is descending or
   ascending (3) for each order, whether the empty set should appear
   first or last.

   The keys can be implementation dependent, but we suggest the keys
   could be a :math:`-` separated string of all the three aspects. For
   example, an order specification list may be

   ``0-ascending-emptyfirst, 1-descending-emptyfirst, 2-ascending-emptylast``

#. Selects with offset and limit

   .. math::

      \inferrule{
              \Delta; \Gamma \vdash e_1 : \tau^{m} \\
              \Delta; \Gamma \vdash e_2 : int^{(\le 1)} \\
              \Delta; \Gamma \vdash e_3 : int_\infty^{(\le 1)} \\
          }{
              \Delta; \Gamma \vdash e_1\,\mathop{\operatorname{\mathit{{offset}}}}\, e_2\,\mathop{\operatorname{\mathit{{limit}}}}\, e_3 : \tau^{[0, ub(m)], mult(m)}
          }

   :math:`int_\infty` is the primitive type of integers with infinity.

   We have a special case for cardinality inference in case :math:`e_3`
   is a primitive integer value

   .. math::

      \inferrule{
              \Delta; \Gamma \vdash e_1 : \tau^{m} \\
              \Delta; \Gamma \vdash e_2 : int^{(\le 1)} \\
              \text{$i$ is an integer}
          }{
              \Delta; \Gamma \vdash e_1\,\mathop{\operatorname{\mathit{{offset}}}}\, e_2\,\mathop{\operatorname{\mathit{{limit}}}}\, i : \tau^{[0, min(ub(m), i)], min(mult(m), i)}
          }

#. Inserts

   .. math::

      \inferrule{\mathop{\operatorname{\mathit{{rt\_type}}}}(T) = \tau \\
       \Delta; \Gamma, x : \tau^{(=1)} \vdash x\, s : \sigma^{(=1)} \\ \sigma \le_i \tau }
      {\Delta, N := T; \Gamma \vdash insert\, N\, s :  \tau^{(=1)}}

   returns the inserted object as a singleton list

   The top level of :math:`s` cannot contain property links.

   The judgment :math:`\sigma \le_i \tau` means that :math:`\sigma` is
   an [index:insert subtype]\ *insert subtype* of :math:`\tau`, in
   particular it may drop object components.

#. Updates

   .. math::

      \inferrule{  \Delta; \Gamma \vdash e : \tau ^{m} \\ \Delta; \Gamma \vdash s : \tau\Rightarrow \tau'
      \\ \tau' \le_i \tau
      \\ \mathop{\operatorname{\mathit{{effect\_free}}}}(e)
      \\ \mathop{\operatorname{\mathit{{effect\_free}}}}(s)
      }
      {\Delta; \Gamma \vdash update\, e\, s :  (\tau')^{m}}

   returns the list of updated objects

   See https://edgedb.slack.com/archives/C04JG7CR04T/p1675383609809549
   for a discussion of the reason for why :math:`e_1` should be effect
   free.

#. Deletes

   .. math::

      \inferrule{  \Delta; \Gamma \vdash e : \tau^{m} 
      \\ \mathop{\operatorname{\mathit{{effect\_free}}}}(e)
      }
      {\Delta; \Gamma \vdash delete\, e :  \tau^{m}}

#. For loops

   .. math::

      \inferrule{\Delta; \Gamma \vdash e_1 : \tau_1^{m_1} \\
                  \Delta; \Gamma , x : \tau_1^{(=1)} \vdash e_2 :  \tau_2^{m_2}}
                  {
                      \Delta; \Gamma \vdash\operatorname{\mathsf{{for}}}(x \leftarrow e_1 ; e_2) :  \tau_2^{m_1 \times m_2}
                  }

#. Optional For loops

   .. math::

      \inferrule{\Delta; \Gamma \vdash e_1 : \tau_1^{[i_1, i_2], i_3} \\
                  \Delta; \Gamma , x : \tau_1^{[max(0, min(i_1, 1)),1], 1} \vdash e_2 :  \tau_2^{m_2}}
                  {
                      \Delta; \Gamma \vdash\operatorname{\mathsf{{optional\_for}}}(x \leftarrow e_1 ; e_2) :  \tau_2^{[max(i_1, min(i_1, 1)), max(i_2, min(i_2,1))], max(i_3, min(i_3, 1)) \times m_2}
                  }

#. If Else

   .. math::

      \inferrule{\Delta; \Gamma \vdash e_1 : bool^{m_1} \\
                   \Delta; \Gamma \vdash e_2 : \tau^{[i_1, i_2], i_3} \\
                   \Delta; \Gamma \vdash e_3 : \tau^{[i_4, i_5], i_6} \\
                  }
                  {
                      \Delta; \Gamma \vdash e_2 \mathop{\operatorname{\mathit{{if}}}} e_1 \mathop{\operatorname{\mathit{{else}}}} e_3 : \tau
                      ^{m_1 \times ([min(i_1, i_4), max(i_2, i_5)], max(i_3, i_6))}
                  }

#. Unnamed Tuple

   .. math::

      \inferrule{ \forall 1 \le i \le n. ( \Delta; \Gamma \vdash e_i : \tau_i^{m_i} )}
          {\Delta; \Gamma \vdash(e_1, \dots,  e_n) : 
          \operatorname{\mathsf{{prod}}}( \tau_1, \dots, \tau_n)^{m_1 \times \dots \times m_n}}

   Unlike objects, tuples are no more or no less.

#. Named Tuple

   .. math::

      \inferrule{ \forall 1 \le i \le n. ( \Delta; \Gamma \vdash e_i : \tau_i^{m_i} )}
          {\Delta; \Gamma \vdash(l_1 := e_1, \dots, l_n := e_n) : 
          \operatorname{\mathsf{{prod}}}(l_1 : \tau_1, \dots, l_n :\tau_n)^{m_1 \times \dots \times m_n}}

   Unlike objects, tuples are no more or no less.

#. Array constructions

   .. math::

      \inferrule{ \forall 1 \le i \le n. ( \Delta; \Gamma \vdash e_i : \tau^{m_i} )}
          {\Delta; \Gamma \vdash[e_1, \dots, e_n] : 
          \operatorname{\mathsf{{arr}}}(\tau)^{m_1 \times \dots \times m_n}}

#. Multiset constructions

   .. math::

      \inferrule{ \forall 1 \le i \le n. ( \Delta; \Gamma \vdash e_i : \tau_i^{m_i} )}
          {\Delta; \Gamma \vdash\{e_1, \dots, e_n\} : 
          (\tau_1 \lor \dots \lor \tau_n)^{m_1 + \dots + m_n}}

Typing Rules for Values
-----------------------

We use the judgments :math:`\Delta \vdash_\mu U : \tau^m`,
:math:`\Delta \vdash_\mu V : \tau`, :math:`\Delta \vdash_\mu W : \tau`
for the typing of values.

#. Typing of value sets

   .. math::

      \inferrule{
              \forall 1 \le i \le n. \Delta \vdash_\mu V_i : \tau
              }{
                  \Delta \vdash_\mu\{V_1, \dots, V_n\}^m : \tau^{(=n)}
              }

   Derived: Typing of Union of operations on value sets

   - Similar to the union of expressions

   .. math::

      \inferrule{
                  \Delta \vdash_\mu U_1 : \tau^{m_1} \\
                  \Delta \vdash_\mu U_2 : \tau^{m_2}
              }{
                  \Delta \vdash_\mu U_1 \cup U_2 : \tau ^{m_1 + m_2}
              }

#. Primitives

   .. math:: \inferrule{  v : \tau }{ \Delta \vdash_\mu v  : \tau}

#. Free or Reference Objects

   .. math::

      \inferrule{ 
                  \Delta \vdash_\mu W : \tau
               }{ 
                  \Delta \vdash_\mu\operatorname{\mathsf{{free}}} : W : \tau
               }

   .. math::

      \inferrule{ 
                      (id, N, W')\in \mu \\ 
                      \Delta \vdash_\mu W' \uplus W : \tau
                   }{ 
                      \Delta \vdash_\mu\operatorname{\mathsf{{ref}}}(id) : W : \tau
                   }

#. Objects - Same as Expressions

   .. math::

      \inferrule{ \forall 1 \le i \le k. (
              {\begin{cases}
                  l = l_j \land \Delta \vdash_\mu V_i : \tau_j^{m_j}   & \text{if $L_i = l$} \\
                  l = l_j' \land \Delta \vdash_\mu V_i : \sigma_j^{m_j'}  &  \text{if $L_i = @l$} \\
              \end{cases}}
                  )}{\Delta \vdash_\mu\{L_1^{u_1} := V_1, \dots, L_k^{u_k} := V_k\} : 
          \{l_1 : \tau_1^{m_1}, \dots, l_n :\tau_n^{m_n}\}
          \mathop{\operatorname{\mathtt{{@}}}}\{l_1' : \sigma^{m_1'}, \dots, l_p' : \sigma^{m_p'}\}}

#. Links with Properties

   .. math::

      \inferrule{ (id, N, W') \in \mu \land N := T \in \Delta \land \mathop{\operatorname{\mathit{{rt\_type}}}}(T) = \tau_1 \\ 
          %  \dvm V_1 : \tau_1 \\
           \Delta \vdash_\mu W_2 : \tau_2
                  }{\Delta \vdash_\mu\operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}W_2  : \tau_1 \mathop{\operatorname{\mathtt{{@}}}}\tau_2}

#. Unnamed Tuple - Same as Expressions

   .. math::

      \inferrule{ \forall 1 \le i \le n. ( \Delta \vdash_\mu V_i : \tau_i )}
              {\Delta \vdash_\mu(V_1, \dots,  V_n) : 
              \operatorname{\mathsf{{prod}}}( \tau_1, \dots, \tau_n)}

   Unlike objects, tuples are no more or no less.

#. Named Tuple - Same as Expressions

   .. math::

      \inferrule{ \forall 1 \le i \le k. ( \Delta \vdash_\mu V_i : \tau_i )}
              {\Delta \vdash_\mu(l_1 := V_1, \dots, l_n := V_n) : 
              \operatorname{\mathsf{{prod}}}(l_1 : \tau_1, \dots, l_n :\tau_n)}

#. Named Tuple - Same as Expressions

   .. math::

      \inferrule{ \forall 1 \le i \le k. ( \Delta \vdash_\mu V_i : \tau )}
             {\Delta \vdash_\mu[V_1, \dots, V_n] : 
             \operatorname{\mathsf{{arr}}}(\tau)}

A dynamic context :math:`\mu` is well-formed with respect to a static
context :math:`\Delta`, if for all :math:`(id, N, W)` in :math:`\mu`,
:math:`\Delta \vdash_\mu \operatorname{\mathsf{{ref}}}(id): W : (\Delta(N))^{(=1)}`.

Metatheories of Typing
----------------------

If :math:`\Delta \vdash_\mu U : \tau^m`,
:math:`\Delta; \Gamma , x : \tau^m \vdash_\mu e : \tau_1^{m'}`, then
:math:`\Delta; \Gamma \vdash_\mu [U/x]e : \tau_1^{m'}`

If :math:`\Delta \vdash_\mu V : \tau`,
:math:`\Delta; \Gamma , x : \tau^m \vdash _\mu e : \tau_1^{m'}`, then
:math:`\Delta; \Gamma \vdash_\mu  [V/x]e  : \tau_1^{m'}`

.. _`sec:dynamics`:

Dynamics
========

Runtime Configuration
---------------------

A *runtime configuration* is written :math:`\mu \parallel^{S}_\Delta e`,
meaning executing command :math:`e` in database :math:`\mu`, with the
current snapshots :math:`S`, and schema :math:`\Delta`. The schema will
be unchanged throughout the execution we will usually omit them.

A [index:runtime snapshot]\ *runtime snapshot* is a sequence of database
stores

.. math:: S ::= \cdot \mid \mu, S

Without transactions, a runtime snapshot is always the current snapshot,
which the select statement reads from, and is written as
:math:`[\mu_0]`.

A runtime configuration :math:`\mu \parallel^{[\mu_0]}_\Delta e` is
well-formed if :math:`\Delta \vdash \mu`, and
:math:`\Delta \vdash \mu_i`, and :math:`\Delta ;\cdot \vdash e : \tau^m`
for some :math:`\tau^m`.

Operational Semantics
---------------------

Using big-step operational semantics, we use the judgment
:math:`\mu \parallel^S e \searrow\mu' \parallel^{S'} U` to mean that
executing query :math:`e` with respect to dynamic context :math:`\mu`
(with runtime snapshots :math:`S`), will produce a new dynamic context
:math:`\mu'` (with read snapshots :math:`S'`) with the result :math:`U`.
To account for automatic link property dereference, we write

.. math:: \mu \parallel^S e \searrow\mu' \parallel^{S'} {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }U

\ to mean that :math:`\mu \parallel^S e \searrow\mu' \parallel^{S'} U'`
and
:math:`\mathop{\operatorname{\mathit{{assume\_link\_target}}}}(U') = U`.

#. Primitive values

   .. math:: \inferrule{ }{ \mu \parallel^S v  \searrow\mu \parallel^S \{v\}}

#. Values

   .. math:: \inferrule{ }{ \mu \parallel^S V  \searrow\mu \parallel^S \{V\}}

#. Value Sets

   .. math:: \inferrule{ }{ \mu \parallel^S U  \searrow\mu \parallel^S U}

#. Type casts

   .. math::

      \inferrule{\mu \parallel^S e \searrow\mu' \parallel^S \{v_1, \dots, v_n\} 
      \\ \forall 1 \le i \le n.\operatorname{\mathsf{{cast}}}(\tau, v_i) = v_i'}{ \mu \parallel^S <\tau>e  \searrow\mu' \parallel^S  \{v_1', \dots, v_n'\}}

   :math:`\operatorname{\mathsf{{cast}}}` is the primitive type cast
   function (operating on primitive values).

   TODO: [is :math:`\tau`] casts : casting of complex values

#. Expressions with Shapes

   .. math::

      \inferrule{\mu \parallel^S e \searrow\mu' \parallel^S \{V_1, \dots V_n\}
      \\
      \forall i \le i \le n . {
          % \mu' \pp^S 
          V_i' = view(s, V_i) 
          % \evalsto \mu' \pp^S V_i' 
       }}
      { \mu \parallel^S e\, s  \searrow\mu'' \parallel^S  \{V_1', \dots, V_n'\}}

   **The View of a Shape on an Object.**

   Let

   (a) the meta-level operation :math:`view(s, V)` denote the result of
       applying a shape :math:`s` on :math:`V`.

   (b) the auxiliary meta-level operation :math:`view(s, W)` denote the
       result of applying a shape :math:`s` on the object value
       :math:`W`.

   (c) the auxiliary meta-level operation :math:`view(c, V)` be the
       object component as a result of applying the shape component
       :math:`c` on value :math:`V`. (Note that :math:`V` remains
       unchanged from the previous function call.)

   The meta-level :math:`view` operations are implicitly parameterized
   by :math:`\mu` and :math:`S`, and are inductively defined as follows:
   (Should this be :math:`\mu'`? I am not sure.)

   #. :math:`view(\{c_1, \dots, c_m\}, V) =` :math:` `

      (1) Assume
      :math:`W = \{L_1^{u_1} := U_1, \dots, L_n^{u_n} := U_n\}`

      We have

      :math:`  `

      (2) It must be the case that
      :math:`W = \{l_1^{u_1} := U_1, \dots, l_n^{u_n} := U_n\}`

      We have :math:` `

      where :math:` `

   #. :math:`view(L := x.e, V) = L^{\mathop{\operatorname{\mathit{{visible}}}}} :=  U' \text{   if } \mu\parallel^S[V/x]e \searrow\mu\parallel^S U'`

   A view on an object (a) computes all properties that are in the
   shapes and set the properties that are not in the shapes to be
   invisible, (b) adds computed properties that are in the shapes but
   not present in the object.

   We have the more intuitive of :math:`view` as a result:

   #. :math:`view(L, V ) = L^{\mathop{\operatorname{\mathit{{visible}}}}} := U'`
      if :math:`( L^u := U') \in V`

   #. :math:`view(l : s, V) = l^{\mathop{\operatorname{\mathit{{visible}}}}} := \{view(s, V') \mid V' \in U'\}`
      if :math:`( l^u := U') \in V`

#. Unions

   .. math::

      \inferrule{\mu \parallel^S e_1 \searrow\mu' \parallel^S U_1 \\ \mu' \parallel^S e_2 \searrow\mu'' \parallel^S U_2 }
          {\mu \parallel^S e_1 \cup e_2 \searrow\mu'' \parallel^S U_1 \cup U_2}

#. Function Calls

   .. math::

      \inferrule{\forall 1 \le i \le n. \mu_{i-1} \parallel^S e_i \searrow\mu_i \parallel^S {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }U_i \\ 
          f : [\tau_1^{p_1}, \dots, \tau_n^{p_n}] \to \tau^m \\
          U' =\cup \{f(x_1, \dots, x_n) : {\begin{cases}
              x_i \in \{\{y_i\} : y_i \in U_i\},  &\text{if}\ p_i = 1  \\
              x_i = \{\},  &\text{if}\ p_i = ? \land U_i = \{\} \\
              x_i \in \{\{y_i\}:  y_i \in U_i\}, & \text{if}\ p_i = ? \land U_i \ne \{\}  \\
              x_i = U_i,  &\text{if}\ p_i = *
          \end{cases}}\}
          }
          {\mu_0 \parallel^S f(e_1, \dots, e_n) \searrow\mu_n \parallel^S U'}

   (Alternative elaboration style semantics :

   .. math::

      \inferrule{\forall 1 \le i \le n. \mu_{i-1} \parallel^S e_i \searrow\mu_i \parallel^S U_i \\ 
          U' = f(U_1, \dots, U_n)}
          {\mu_0 \parallel^S f(e_1, \dots, e_n) \searrow\mu_n \parallel^S U'}

   )

#. Object Construction

   .. math::

      \inferrule{\forall 1 \le i \le n. \mu_{i-1} \parallel^S e_i \searrow\mu_i \parallel^S U_i \\ 
         }
          {\mu_0 \parallel^S \{n_1 := e_1, \dots n_n := e_n\} 
          \searrow\mu_n \parallel^S\{ \operatorname{\mathsf{{free}}}: \{n_1^{\mathop{\operatorname{\mathit{{visible}}}}} := U_1, \dots, n_n^{\mathop{\operatorname{\mathit{{visible}}}}} := U_n\}\}}

#. Object Projection

   Define the singular projection function:

   :math:`proj(L, \operatorname{\mathsf{{free}}} :\{L_1^{u_1} := U_1, \dots, L_n^{u_n} := U_n\}) =`

   :math:` `

   :math:`proj(\operatorname{\mathsf{{id}}}, \operatorname{\mathsf{{ref}}}(id) :\{L_1^{u_1} := U_1, \dots, L_n^{u_n} := U_n\}) = id`

   :math:`proj(L, \operatorname{\mathsf{{ref}}}(id) :\{L_1^{u_1} := U_1, \dots, L_n^{u_n} := U_n\}) =`

   :math:`\ \ \ \ \ \ {\begin{cases}
   U_i & \text{if } l = L_i \\
   U_j' & \text{otherwise, if } S = [\mu_0] \land (\operatorname{\mathsf{{ref}}}(id), N, \{\dots, L^{u_j'} := U_j', \dots\}) \in \mu_0\\
   U_j' & \text{otherwise, if } S = [\mu_0] \land (\operatorname{\mathsf{{ref}}}(id), N, \dots) \in \mu_0 \land N := T \land
   \\ & \hspace{3em} L := \operatorname{\mathsf{{comp(x.e, M)}}} \in T \land \mu \parallel^S [\operatorname{\mathsf{{ref}}}(id): \{\dots\}/x]e \searrow\mu\parallel^S U_j'\\
    \textcolor{teal}{error} & \text{otherwise }
       \end{cases}}`

   :math:`proj(l_i, (l_1 := V_1, \dots, l_n := V_n)) = \{V_i\}`

   :math:`proj(i, (l_1 := V_1, \dots, l_n := V_n)) = \{V_i\}` (:math:`i`
   is a number between :math:`1` and :math:`n`)

   :math:`proj(i, (V_1, \dots, V_n)) = \{V_i\}` (:math:`i` is a number
   between :math:`1` and :math:`n`)

   :math:`proj(l_i, \operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}W) = proj(l_i, \operatorname{\mathsf{{ref}}}(id): \{\})`

   :math:`proj(@l_i, \operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}W) = proj(l_i, \operatorname{\mathsf{{free}}}: W)`

   .. math::

      \inferrule{\mu \parallel^S e \searrow\mu' \parallel^S {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }\{V_1, \dots, V_n\}}
          {\mu \parallel^S e \cdot l \searrow\mu' \parallel^S 
          % {\begin{cases}
              \cup_{1\le j \le n} \ proj(l, V_j) \\
          }
              % proj(l, V_1)  \ \ \text{otherwise, and $n = 1$} \\
              % \cup_{1 \le j \le n}\ \mi{convert\_to\_link}(proj(l,V_j)) 
              % \\\hspace{10em}
              % \ \ \text{all $proj(l, V_j)$ are link convertible} \\
              % \cup_{1 \le j \le n}\ proj(l,V_j) \ \ \text{otherwise} \\
              % unique(\ (\mi{remove\_link\_prop} ()) ) \\
              % \hspace{10em}\text{otherwise, and $n \gt 1$} \\
          % \end{cases}}}

   Essentially, data can be read from the database if it is a reference.

#. Link Property Projections

   .. math::

      \inferrule{
                  \mu \parallel^S e  \searrow\mu' \parallel^S \{V_1, \dots, V_n\}
              }{
                  \mu \parallel^S e \mathop{\operatorname{\mathtt{{@}}}}l \searrow\mu' \parallel^ S \cup _{1\le i \le n}\{proj(@l, V_i)\}
              }

#. Backlinks

   .. math::

      \inferrule{
                  \mu \parallel^{[\mu_0]} e  \searrow\mu' \parallel^{[\mu_0]} 
                  {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }\{\operatorname{\mathsf{{ref}}}(id_1) : W_1, \dots, \operatorname{\mathsf{{ref}}}(id_n) : W_n\}
              }{
                  \mu \parallel^{[\mu_0]} e \mathop{\operatorname{\mathtt{\cdot_{\leftarrow}}}}l \searrow
                  \mu' \parallel^ S \{\operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}W' \mid (id, N, W)\in \mu_0, 
                  \\ \exists i. W =  \{\dots, l^u := \{\dots, \operatorname{\mathsf{{ref}}}(id_i) \mathop{\operatorname{\mathtt{{@}}}}W', \dots\}, \dots\}\}
              }

   Free Objects?

#. Type intersections

   .. math::

      \inferrule{
                      \mu \parallel^{[\mu_0]}_\Delta e  \searrow
                      \mu' \parallel^{[\mu_0]}_\Delta
                      {\begin{cases}
                  \{\operatorname{\mathsf{{ref}}}(id_1) : W_1, \dots, \operatorname{\mathsf{{ref}}}(id_n) : W_n\}  \\
                  \text{or }\{\operatorname{\mathsf{{ref}}}(id_1) \mathop{\operatorname{\mathtt{{@}}}}W_1, \dots, \operatorname{\mathsf{{ref}}}(id_n)\mathop{\operatorname{\mathtt{{@}}}}W_n\}  \\
                      \end{cases}}
                  \\
                  U =  \{
                      {\begin{cases}
                      \operatorname{\mathsf{{ref}}}(id_i) : W_i  \\
                      \text{or }\operatorname{\mathsf{{ref}}}(id_i) \mathop{\operatorname{\mathtt{{@}}}}W_i 
                      \end{cases}}
                      \mid 
                  \forall_{1\le i\le n}.{
                      \begin{cases}
                          N' \le  N & \text{ if} (id_i, N', W') \in \mu' \\
                          N' \le  N & \text{ if} (id_i,\_, \_) \notin \mu' \land (id_i, N', W') \in \mu_0 \\
                      \end{cases}
                  }
                  \}
                  }{
                      \mu \parallel^{[\mu_0]}_\Delta e \ {[is\, {N}]} 
                      \searrow
                      \mu' \parallel^{[\mu_0]}_\Delta U
                  }

   Free Objects? Pershaps: we don’t need to read from :math:`\mu_0`
   (current implementation), what about deletes?

#. Selects

   .. math::

      \inferrule{
                  \mu \parallel^S e \searrow\mu \parallel^S U
              }{
                  \mu \parallel^S \mathop{\operatorname{\mathit{{select}}}}\, e \searrow\mu \parallel^S U
              }

#. Detached Expressions

   .. math::

      \inferrule{
                  \mu \parallel^S e \searrow\mu \parallel^S U
              }{
                  \mu \parallel^S \mathop{\operatorname{\mathit{{detached}}}}\, e \searrow\mu \parallel^S U
              }

#. With Bindings

   .. math::

      \inferrule{\mu \parallel^S e_1 \searrow\mu' \parallel^S U_1 \\
          \mu' \parallel^S [U_1/x]e_2 \searrow\mu'' \parallel^S U_2}
          {\mu \parallel^S \operatorname{\mathsf{{with}}}(x := e_1; e_2) \searrow\mu'' \parallel^S U_2}

#. Type Names

   .. math:: \inferrule{ }{\mu \parallel^{[\mu_0]}  N \searrow\mu\parallel^{[\mu_0]} \{\operatorname{\mathsf{{ref}}}(id) : \{\} \mid (id, N,  W) \in \mu_0\}}

#. Selects with filter and order

   .. math::

      \inferrule{
                  \mu \parallel^S e_1
                  \searrow
                  \mu' \parallel^S \{ V^{\mathop{\operatorname{\mathit{{selected}}}}}_1, \dots, V^{\mathop{\operatorname{\mathit{{selected}}}}}_n\} \\ 
                  \forall  i . \mu' \parallel^S [V^{\mathop{\operatorname{\mathit{{selected}}}}}_i/x]e_2 \searrow\mu' \parallel^S \{V^{\mathop{\operatorname{\mathit{{cond}}}}}_i\} \ \ \ \ \ 
                  U^{\mathop{\operatorname{\mathit{{cond-ed}}}}} = \{V_i^{\mathop{\operatorname{\mathit{{selected}}}}}  \mid true \in V^{\mathop{\operatorname{\mathit{{cond}}}}}_i, 1 \le i \le n\} \\
                  U^{\mathop{\operatorname{\mathit{{cond-ed}}}}} = \{V_1^{\mathop{\operatorname{\mathit{{cond-ed}}}}}, \dots, V_m^{\mathop{\operatorname{\mathit{{cond-ed}}}}}\} \\
                  \forall  j . \mu' \parallel^S [V^{\mathop{\operatorname{\mathit{{cond-ed}}}}}_j/x] e_3 \searrow\mu' \parallel^S \{V^{\mathop{\operatorname{\mathit{{order}}}}}_j\} \ \ \ \ \ 
                  U^{\mathop{\operatorname{\mathit{{ordered}}}}} = \mathop{\operatorname{\mathit{{orderby}}}} (U^{\mathop{\operatorname{\mathit{{cond-ed}}}}}, \{V^{\mathop{\operatorname{\mathit{{order}}}}}_1, \dots, V^{\mathop{\operatorname{\mathit{{order}}}}}_m\}) \\
              }{
                  \mu \parallel^S e_1\, \mathop{\operatorname{\mathit{{filter}}}}\, (x.e_2)\, \mathop{\operatorname{\mathit{{order}}}}\, (x.e_3)
                  \searrow\mu' \parallel^S U^{\mathop{\operatorname{\mathit{{ordered}}}}}
              }

   where :math:`orderby(V_1, V_2)` orders the set :math:`V_1` by the
   value :math:`V_2` (of the same length as :math:`V_1`). The
   implementation will read all keys of the object and sort according to
   the order specification list read from the list of keys.

#. Selects with offset and limit.

   .. math::

      \inferrule{
                  \mu \parallel^S e_1
                  \searrow
                  \mu' \parallel^S U^{\mathop{\operatorname{\mathit{{selected}}}}} \\ 
                  {\begin{cases}
                  \mu' \parallel^S e_2 \searrow\mu'' \parallel^S \{V_{\mathop{\operatorname{\mathit{{offset}}}}}\} 
                  &\text{then } U^{\mathop{\operatorname{\mathit{{offset-ed}}}}} = \mathop{\operatorname{\mathit{{offset}}}}(U^{\mathop{\operatorname{\mathit{{selected}}}}}, V_{\mathop{\operatorname{\mathit{{offset}}}}}) \\ 
                  \text{or if }\mu' \parallel^S e_2 \searrow\mu'' \parallel^S \{\} 
                  &\text{then } U^{\mathop{\operatorname{\mathit{{offset-ed}}}}} = U^{\mathop{\operatorname{\mathit{{selected}}}}}
                  \end{cases}} \\
                  {\begin{cases}
                  \mu'' \parallel^S e_3 \searrow\mu''' \parallel^S \{V_{\mathop{\operatorname{\mathit{{limit}}}}}\} 
                  &\text{then } U^{\mathop{\operatorname{\mathit{{limited}}}}} = \mathop{\operatorname{\mathit{{limit}}}}(U^{\mathop{\operatorname{\mathit{{offset-ed}}}}}, V_{\mathop{\operatorname{\mathit{{limit}}}}}) \\
                  \text{or if }\mu'' \parallel^S e_3 \searrow\mu''' \parallel^S \{\} 
                  &\text{then } U^{\mathop{\operatorname{\mathit{{limited}}}}} = U^{\mathop{\operatorname{\mathit{{offset-ed}}}}}
                  \end{cases}
                  }
              }{
                  \mu \parallel^S  e_1\, \mathop{\operatorname{\mathit{{offset}}}}\, e_2\,\mathop{\operatorname{\mathit{{limit}}}}\, e_3 
                  \searrow\mu''' \parallel^S U^{\mathop{\operatorname{\mathit{{limited}}}}}
              }

   where :math:`\mathop{\operatorname{\mathit{{offset}}}}(U_1, v)`
   throws away :math:`v` (int) elements of :math:`U_1`

   :math:`\mathop{\operatorname{\mathit{{limit}}}}(U_1, v)` throws away
   all elements of :math:`U_1` after the :math:`v`\ th elements

#. Inserts

   .. math::

      \inferrule{
               \mu \cup {(id, N, \{\})}\parallel^S (\operatorname{\mathsf{{ref}}}(id) : \mathop{\operatorname{\mathit{{initial_T}}}})\, ({step_T})^n\, s\, (\mathop{\operatorname{\mathit{{post\_step_T^s}}}})^n
              \\ \searrow\mu' \cup {(id,N, \{\})}  \parallel^S \operatorname{\mathsf{{ref}}}(id): W' \\
              \\ N := T \in \Delta \\ W' \lhd T \Rightarrow W'' : \Delta 
               }{\mu \parallel^S insert\, N\, s \searrow\mu' \cup \{(id, N, W'' )\}\parallel^S \{\operatorname{\mathsf{{ref}}}(id): \{\}\}} \text{($id$ fresh)}

   The strange default computations need significant discussions (TODO:
   we may want to support inserting references? The current surface
   syntax only allows free objects).

   We coerce first before evaluating the argument. (This may be changed
   to coerce on values. Coercion does make material changes besides
   adding empty sets to the top level). We again identify types w.r.t.
   :math:`\Delta`.

   **Storage Coercion.**

   We use the judgment :math:`W \lhd  T \Rightarrow W' : \Delta` to
   coerce from object :math:`W` with schema type :math:`T`, and produces
   the result :math:`W'`. Storage coercion attempts to fill undefined
   properties with the empty set, and also make links suitable for
   storing.

   Examples of object coercion:

   .. math:: \{ name := \{2\} \} \lhd \{name :  int^{(=1)}, age : int^{(\le 1)} \}

   .. math:: \ \ \ \ \ \ \ \ \ \ \ \Rightarrow \{ name := \{2\}, age := \{\}\} : \Delta

   .. math:: \{ do not exist := \{2\} \} \lhd \{name :  int^{(=1)}, age : int^{(<=1)} \}  \not\Rightarrow

   Rules for object coercion:

   (The second premise ensure that there are no redundant labels in an
   object)

   Given :math:`T = \{l_1 : M_1^{m_1}, \dots l_n : M_n^{m_n} \}`,
   :math:`\mu` and :math:`S`, we define the following auxiliary
   operations for a schema object type.

   :math:`\mathop{\operatorname{\mathit{{initial}}}}_T` is the initial
   object value of :math:`T`.

   .. math::

      \mathop{\operatorname{\mathit{{initial_T}}}} = 
      \forall_{1 \le j \le n}. \uplus
          {\begin{cases}
              % l_j := e & \text{if } M_j = \m{default}(x.e, M_j') \land \text{$x$ is unbound in $e$} \\
              \textcolor{teal}{empty} & \text{if } M_j = \operatorname{\mathsf{{comp}}}(x.e, M_j') \\
              l_j^{visible} := \{\} & \text{otherwise}
          \end{cases}
          }

   :math:`\mathop{\operatorname{\mathit{{step}}}}_T` is the shape that
   populates things with defaults

   .. math::

      \mathop{\operatorname{\mathit{{step_T}}}} = 
      \forall_{1 \le j \le n}. \uplus
          {\begin{cases}
              l_j := x.e & \text{if } M_j = \operatorname{\mathsf{{default}}}(x.e, M_j')  \\
              \textcolor{teal}{empty} & \text{if } M_j = \operatorname{\mathsf{{comp}}}(x.e, M_j') \\
              l_j := \{\} & \text{otherwise} 
          \end{cases}
          }

   In a non-recursive non-effectful default value computations,
   iterating :math:`step` :math:`n` times is sufficient for the
   population of all values.

   Moreover, given an insert shape :math:`s`, define the
   :math:`\mathop{\operatorname{\mathit{{post\_step}}}}` that populates
   values that are not populated by the shape :math:`s`.

   .. math::

      \mathop{\operatorname{\mathit{{post\_step_T^s}}}} = 
      \forall_{1 \le j \le n}. \uplus
          {\begin{cases}
              l_j := x.e & \text{if } M_j = \operatorname{\mathsf{{default}}}(x.e, M_j') \land l_j := \dots \notin s \\
              \textcolor{teal}{empty} & \text{otherwise}
          \end{cases}
          }

   .. math::

      \inferrule{
              \forall_{1 \le j \le n,  M_j \ne \operatorname{\mathsf{{comp}}}(x.e, M_j)}. 
               U_j' = {\begin{cases}
                  \mathop{\operatorname{\mathit{{make\_storage}}}}(U_i, M_j\textcolor{red}{^{m_j}}) , \text{if }\exists i.  l_j = L_i   \\
                      \{\}, \text{otherwise}
                  \end{cases}}
              \\
              \forall 1 \le i \le k. \exists 1 \le j \le n. L_i = l_j \land M_j \ne \operatorname{\mathsf{{comp}}}(x.e, M_j')
              \\
          }{
               \{L_1 := U_1, \dots L_k := U_k\} \lhd \{l_1 : M_1^{m_1}, \dots l_n : M_n^{m_n} \}
              \\
              \Rightarrow 
                  \{l_1 := U_1', \dots, l_n := U_n'\}
                  : \Delta
          }

   Note that we are overloading the syntax category :math:`c` for object
   components.

   TODO: Insert Constraint and Cardinality Check

   Make storage will make a value suitable for storage, it will drop
   objects attached to references and keep others.

   .. math::

      \mathop{\operatorname{\mathit{{make\_storage}}}}(\{V_1, \dots V_n\}, t_j) = 
              \cup_i (\mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V_i, t_j))

   .. math::

      \begin{array}{l}
          \mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V, t) = V \text{  if $t$ is primitive}\\
          \mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V, t\mathop{\operatorname{\mathtt{{@}}}}T) = 
          \hspace{10em}\,\, 
          \\
          \hspace{5em}
          \begin{cases}
              \operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}W_4
              \\ 
                 \hspace{1em}  \text{if } V = \operatorname{\mathsf{{ref}}}(id) : W_1 
              %    \text{ and all keys of $W_1$ are link property labels ($@l$)}
                 \\
                 \hspace{1em}  
                  W_2 = \mathop{\operatorname{\mathit{{remove\_unless\_link\_prop}}}}(W_1)
                 \\
                 \hspace{1em}  
                  W_3 = \mathop{\operatorname{\mathit{{link\_prop\_obj\_to\_obj}}}}(W_2)
                 \\
                 \hspace{1em}  
                  W_3 \lhd T \Rightarrow W_4 : \Delta
                  \\ 
              \operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}W' \hspace{1em} \text{if } V = \operatorname{\mathsf{{ref}}}(id) \mathop{\operatorname{\mathtt{{@}}}}W \land W \lhd T \Rightarrow W'\\
              \textcolor{teal}{error} \hspace{14em} \text{otherwise} \\
          \end{cases} \\
          \mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V, t) =\mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V, t\mathop{\operatorname{\mathtt{{@}}}}\{\})  \text{  if $t$ is not primitive}\\
          \mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V, \operatorname{\mathsf{{default}}}(x.e, M)) =\mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V, M)  \\
          \mathop{\operatorname{\mathit{{make\_storage\_atomic}}}}(V, \operatorname{\mathsf{{comp}}}(x.e, M)) = \textcolor{teal}{error} \\
              \end{array}

   Note
   :math:`\mathop{\operatorname{\mathit{{remove\_unless\_link\_prop}}}}`
   implies that we allow extra other keys present in :math:`W_1`, but
   not extra link property keys.

#. Updates

   .. math::

      \inferrule{\mu' \parallel^S e \searrow\mu' \parallel^S {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }\{\operatorname{\mathsf{{ref}}}(id_1) : W_1, \dots, \operatorname{\mathsf{{ref}}}(id_n) : W_n\}
          \\ \forall i.\, \mu' \parallel^S (\operatorname{\mathsf{{ref}}}(id_i) : W_i)\, s \searrow\mu' \parallel^S \{V_i\} \land V_i = \operatorname{\mathsf{{ref}}}(id_i): W_i'  }
          {\mu'\parallel^S update\, e\, s \searrow\mu''\parallel^S \{V_1, \dots, V_n\}}

   where
   :math:`\mu'' = \{(id, N, W) \mid (id, N, W) \in \mu' \land id \ne id_i\}`

   :math:`\cup \{(id_i, N, W_i'') \mid id = id_i \land (id, N, W)\in \mu' \land N := T \in \Delta \land 
   (W \uplus W_i') \lhd T \Rightarrow W_i'' \}`

   Note: because of read snapshots, :math:`W_i` may be different from
   :math:`W` in the second clause.

#. Deletes

   .. math::

      \inferrule{\mu' \parallel^S e \searrow\mu' \parallel^S  {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }\{\operatorname{\mathsf{{ref}}}(id_1) : W_1, \dots, \operatorname{\mathsf{{ref}}}(id_n) : W_n\} }
          {\mu'\parallel^S delete\, e \searrow\mu''\parallel^S U}

   where
   :math:`\mu'' = \{(id, N, W) \mid (id, N, W) \in \mu' \land id \ne id_i\}`

   and
   :math:`U = \{\operatorname{\mathsf{{ref}}}(id_i) : W_i \mid \forall _i, 1 \le i \le n.   \exists (id, N, W) \in \mu' \land id = id_i\}`

#. For loops

   .. math::

      \inferrule{ 
              \mu \parallel^S e_1 \searrow\mu_0 \parallel^S \{V_1, \dots V_n\} 
          \\
          \forall 1 \le i \le n.\,  \mu_{i-1} \parallel^S [V_i/x] e_2 \searrow\mu_i \parallel^S  U_i'
          }
          {\mu \parallel^S \operatorname{\mathsf{{for}}}(x \leftarrow e_1; e_2) \searrow\mu_n \parallel^S U_1' \cup \dots \cup U_n'}

#. Optional for loops

   .. math::

      \inferrule{ \mu \parallel^S e_1 \searrow\mu' \parallel^S \{\} 
          \\
            \mu' \parallel^S [\{\}/x] e_2 \searrow\mu'' \parallel^S  U
          }
          {\mu \parallel^S \operatorname{\mathsf{{optional\_for}}}(x \leftarrow e_1; e_2) \searrow
           \mu'' \parallel^S U}

   .. math::

      \inferrule{ \mu \parallel^S e_1 \searrow\mu_0 \parallel^S \{V_1, \dots V_n\} 
          \\
          \forall 1 \le i \le n.\,  \mu_{i-1} \parallel^S [V_i/x] e_2 \searrow\mu_i \parallel^S  U_i'
          }
          {\mu \parallel^S \operatorname{\mathsf{{optional\_for}}}(x \leftarrow e_1; e_2) \searrow\mu_n \parallel^S U_1' \cup \dots \cup U_n'}

#. If Else

   .. math::

      \inferrule{
              \mu \parallel^S e_1 \searrow\mu_0 \parallel^S \{V_1, \dots V_n\} 
          \\
          \forall 1 \le i \le n.\,  
          {\begin{cases}
              \mu_{i-1} \parallel^S  e_2 \searrow\mu_i \parallel^S  U_i' &  \text{if }  V_i = \mathop{\operatorname{\mathit{{true}}}} \\
              \mu_{i-1} \parallel^S  e_3 \searrow\mu_i \parallel^S  U_i' &  \text{if }  V_i = \mathop{\operatorname{\mathit{{false}}}}
          \end{cases}
          }
              }{
              \mu \parallel^S e_2 \mathop{\operatorname{\mathit{{if}}}} e_1 \mathop{\operatorname{\mathit{{else}}}} e_3\searrow\mu_n \parallel^S U_1' \cup \dots \cup U_n'
              }

#. Unnamed Tuples

   .. math::

      \inferrule{\forall 1 \le i \le n. \mu_{i-1} \parallel^S e_i \searrow\mu_i \parallel^S {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }U_i \\ 
         }
          {\mu_0 \parallel^S ( e_1, \dots  e_n)
          \searrow\mu_n \parallel^S \{( V_1, \dots,  V_n) \mid V_1 \in U_1, \dots, V_n \in U_n\}}

#. Named Tuples

   .. math::

      \inferrule{\forall 1 \le i \le n. \mu_{i-1} \parallel^S e_i \searrow\mu_i \parallel^S {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }U_i \\ 
         }
          {\mu_0 \parallel^S (l_1 := e_1, \dots l_n := e_n)
          \searrow\mu_n \parallel^S \{(l_1 := V_1, \dots, l_n := V_n)\mid V_1 \in U_1, \dots, V_n \in U_n\}}

#. Arrays

   .. math::

      \inferrule{\forall 1 \le i \le n. \mu_{i-1} \parallel^S e_i \searrow\mu_i \parallel^S {\overset{\mathop{\operatorname{\mathit{{tgt}}}}}{\rightharpoonup}\ }U_i \\ 
         }
          {\mu_0 \parallel^S [e_1, \dots, e_n]
          \searrow\mu_n \parallel^S \{[V_1, \dots, V_n]\mid V_1 \in U_1, \dots, V_n \in U_n\}}

#. Multiset Constructions

   .. math::

      \inferrule{\forall 1 \le i \le n. \mu_{i-1} \parallel^S e_i \searrow\mu_i \parallel^S U_i \\ 
         }
          {\mu_0 \parallel^S \{e_1, \dots, e_n\}
          \searrow\mu_n \parallel^S U_1 \cup \dots \cup U_n}

Safety
======

The execution should be type safe, except the following:

See red texts and TODO’s

.. _`sec:preprocessing`:

IR Preprocessing
================

IR preprocessing happens before IR type checking and execution, and
after elaboration from surface syntax into IR.

The main part of IR-Preprocessing is the hoisting of names and paths.

Query, Subqueries, and Semi-Subqueries
--------------------------------------

Any expression is a query. We write :math:`e_1 \prec e_2` to mean that
:math:`e_1` is a subquery of :math:`e_2` and :math:`e_1 \sim e_2` to
mean that :math:`e_1` is in the top level of :math:`e_2`.

A subexpression is not always a subquery. For example, in general,
:math:`E` is not a subquery of :math:`E.l`.

Subqueries affect how names are factored inside a query.

Semi-Subquery
-------------

A semi subquery affects the binding structure but only in certain
circumstances. We use :math:`e_1 \preceq e_2` to denote that :math:`e_1`
is a semi-subquery of :math:`e_2`.

There is currently only one kind of semi-subquery introduced by optional
arguments (see the case for functions calls below).

#. :math:`e_i \prec \{e_1, \dots e_n\} \ (1 \le i \le n)`, i.e.
   :math:`e_i \prec e_1 \cup e_2 \ (i = 1, 2)`

#. :math:`e_i \prec f(e_1, \dots e_n)`, if
   :math:`f : [\tau_1^{p_1}, \dots, \tau_n^{p_n}] \to \tau^m` and
   :math:`p_i = *`

#. :math:`s \prec e\, s`

#. Let
   :math:`e =  e_1\, \mathop{\operatorname{\mathit{{filter}}}}\, (x.e_2)\, \mathop{\operatorname{\mathit{{order}}}}\, (x.e_3)`,
   we have

   #. :math:`e_2 \prec e`,

   #. :math:`e_3 \prec e`,

   Note that :math:`e_1 \not\prec e`

#. Let
   :math:`e =  e_1\, \mathop{\operatorname{\mathit{{offset}}}}\, e_2\, \mathop{\operatorname{\mathit{{limit}}}}\, e_3`,
   then :math:`e_1, e_2, e_3 \prec e`.

#. if :math:`s = \{\dots, n_i := e_i, \dots \}`, then
   :math:`e_i \prec s`

#. (Transitivity) :math:`e_1 \prec e_3` if there exists :math:`e_2` such
   that :math:`e_1 \prec e_2` and :math:`e_2 \prec e_3`.

+----------------------------------+----------------------------------+
| :math:`e`                        | Top Level or Subquery            |
+==================================+==================================+
| :math:`v`                        |                                  |
+----------------------------------+----------------------------------+
| :math:`V`                        |                                  |
+----------------------------------+----------------------------------+
| :math:`U`                        |                                  |
+----------------------------------+----------------------------------+
| :math:`<\tau>(e_1)`              | :math:`e_1 \sim e`               |
+----------------------------------+----------------------------------+
| :math:`\{e_1, \dots , e_n\}`     | :math:`\fora                     |
|                                  | ll_{1 \le i \le n}. e_i \prec e` |
+----------------------------------+----------------------------------+
| :math:`\{                        | :math:`\fora                     |
| L_1 := e_1, \dots, L_n := e_n\}` | ll_{1 \le i \le n}. e_i \prec e` |
+----------------------------------+----------------------------------+
| :math:`e'\, s = e'\, \{L_1       | :math:`e' \sim e`,               |
| := x.e_1, \dots, L_n := x.e_n\}` | :math:`\fora                     |
|                                  | ll_{1 \le i \le n}. e_i \prec e` |
+----------------------------------+----------------------------------+
| :math:`e_1 \cup e_2`             | :math:`e_1 \prec e, e_2 \prec e` |
+----------------------------------+----------------------------------+
| :math:`f(e_1, \dots, e_n)`       |                                  |
+----------------------------------+----------------------------------+
| :math:`\fora                     |                                  |
| ll_{1 \le i \le n}.\begin{cases} |                                  |
| e_i \s                           |                                  |
| im e  & \text{if }    p_i = 1 \\ |                                  |
| e_i \prec                        |                                  |
| eq e  & \text{if }    p_i = ? \\ |                                  |
| e_i \pr                          |                                  |
| ec e  & \text{if }    p_i = * \\ |                                  |
|     \end{cases}`                 |                                  |
+----------------------------------+----------------------------------+
| :math:`x`                        |                                  |
+----------------------------------+----------------------------------+
| :math:`e_1 \cdot l`              | :math:`e_1 \sim e`               |
+----------------------------------+----------------------------------+
| :math:`\operatorname{            | :math:`e_1 \prec e, e_2 \prec e` |
| \mathsf{{with}}}(x := e_1; e_2)` |                                  |
+----------------------------------+----------------------------------+
| :math:`N`                        |                                  |
+----------------------------------+----------------------------------+
| :math:`e_1 \matho                | :math:`e_1 \sim e`               |
| p{\operatorname{\mathtt{{@}}}}l` |                                  |
+----------------------------------+----------------------------------+
| :math:`e_1 \mathop{\operatorname | :math:`e_1 \sim e`               |
| {\mathtt{\cdot_{\leftarrow}}}}l` |                                  |
+----------------------------------+----------------------------------+
| :math:`e_1 \ {[is\, {N}]}`       | :math:`e_1 \sim e`               |
+----------------------------------+----------------------------------+
| :math:`select\, e_1`             | :math:`e_1 \prec e`              |
+----------------------------------+----------------------------------+
| :math:`detached\, e_1`           | (special)                        |
+----------------------------------+----------------------------------+
| :math:`e_1\, \mathop{            | :math:`e_1 \                     |
| \operatorname{\mathit{{filter}}} | sim e, e_2 \prec e, e_3 \prec e` |
| }\, (x.e_2)\,\mathop{\operatorna |                                  |
| me{\mathit{{order}}}}\, (x.e_3)` |                                  |
+----------------------------------+----------------------------------+
| :math:`e_1\, \m                  | :math:`e_1 \prec e`,             |
| athop{\operatorname{\mathit{{off | :math:`e_2 \prec e, e_3 \prec e` |
| set}}}}\, e_2 \, \mathop{\operat |                                  |
| orname{\mathit{{limit}}}}\, e_3` |                                  |
+----------------------------------+----------------------------------+
| :math:`insert\, N\, e_1`         | :math:`e_1 \sim e`               |
+----------------------------------+----------------------------------+
| :math:`update \, e_1\, s`        | :math:`e_1 \sim e`,              |
|                                  | :math:`s \prec e`                |
+----------------------------------+----------------------------------+
| :math:`delete \, e_1`            | :math:`e_1 \sim e`               |
+----------------------------------+----------------------------------+
| :math:`\operatorname{\mathsf{    | :math:`e_1 \prec e, e_2 \prec e` |
| {for}}}(x \leftarrow e_1;  e_2)` |                                  |
+----------------------------------+----------------------------------+
| :math:                           | :math:`e_1 \prec e, e_2 \prec e` |
| `\operatorname{\mathsf{{optional |                                  |
| \_for}}}(x \leftarrow e_1; e_2)` |                                  |
+----------------------------------+----------------------------------+
| :                                | :math:`e_1 \                     |
| math:`e_2 \mathop{\operatorname{ | sim e, e_2 \prec e, e_3 \prec e` |
| \mathit{{if}}}} e_1 \mathop{\ope |                                  |
| ratorname{\mathit{{else}}}} e_3` |                                  |
+----------------------------------+----------------------------------+
| :math:`(e_1, \dots, e_n)`        | :math:`\fora                     |
|                                  | ll _{1 \le i \le n}. e_i \sim e` |
+----------------------------------+----------------------------------+
| :math:`                          | :math:`\fora                     |
| (l_1 := e_1, \dots, l_n := e_n)` | ll _{1 \le i \le n}. e_i \sim e` |
+----------------------------------+----------------------------------+
| :math:`[e_1, \dots, e_n]`        | :math:`\fora                     |
|                                  | ll _{1 \le i \le n}. e_i \sim e` |
+----------------------------------+----------------------------------+

Paths and Path Substitutions
----------------------------

As a preprocessing step, all paths are preprocessed in a single batch.
Before processing any subqueries. A [index:path]\ *path* :math:`P` is a
name :math:`N` or :math:`x` followed by a list of labels :math:`l`, i.e.
:math:`N \cdot l_1\mathop{\operatorname{\mathtt{\cdot_{\leftarrow}}}}l_2 \dots\mathop{\operatorname{\mathtt{{@}}}}l_n`
or
:math:`x\mathop{\operatorname{\mathtt{{@}}}}l_1.\dots\mathop{\operatorname{\mathtt{\cdot_{\leftarrow}}}}l_n`
that is maximal, in the sense that there are no more trailing
:math:`\cdot l` or
:math:`\mathop{\operatorname{\mathtt{\cdot_{\leftarrow}}}}l` or
:math:`\mathop{\operatorname{\mathtt{{@}}}}l` following a path. Let
:math:`R` denote the path head.

We may need to replace all particular paths in a query by another
expression, for instance, when translating from the query
:math:`(X\cdot \mathop{\operatorname{\mathit{{first}}}}, X \cdot \mathop{\operatorname{\mathit{{last}}}})`,
we may replace :math:`X.\mathop{\operatorname{\mathit{{first}}}}` by
:math:`Y`, and getting
:math:`(Y, X\cdot \mathop{\operatorname{\mathit{{last}}}})`. We denote
such substitution by :math:`<e_1/P>e_2`, which replaces path :math:`P`
in :math:`e_2` (except those appear in a detached subexpression) by
:math:`e_1`.

Select Raising (Hoisting)
-------------------------

Pre-Top-Levels and Top-Levels of a Query
--------------------------------------------

.. image:: images/top-level-illustration2.eps
   :alt: image

The *pre-top-level* of :math:`e` are positions in :math:`e` that do not
belong to any of its subqueries. A path :math:`P` *appears in the
pre-top-level* of :math:`e` if it appears in :math:`E` and such
occurrence is not in any :math:`e' \prec e` (but may occur in
semi-subqueries, i.e. :math:`e'' \preceq e`). A path :math:`P` *appears
in the (proper) top-level* if it appears in the pre-top-level, and the
path :math:`P` and all its prefixes do not solely appear in the same
semi-subquery (and not elsewhere in the pre-top-level and subqueries) of
:math:`e`. As an example, in the query

``(Person.first_name ++ Person.last_name) ?? <name>``

where :math:`??` is the builtin-function of type
:math:`[str^{?}, str^{1}] \to str^{(=1)}` and :math:`(,)` is the
built-in tuple construction function of type
:math:`[\tau_1^1, \tau_2^1] \to prod(\tau_1, \tau_2)^{(=1)}`, has
pre-top-level paths ``Person.first_name`` and ``Person.last_name``, but
has no top-level path because both names appear solely in the same
subquery and all the paths together with their prefixes only occur in
this semi-subquery. On the other hand, in the query

``(Person.first_name ?? "<fst>")``

``++ (Person.last_name ?? "<lst>")``

both paths ``Person.first_name`` and ``Person.last_name`` occur in the
(proper) top-level both paths share a prefix ``Person`` that appear
elsewhere in the pre-top-level.

A *hoisting prefix* :math:`P_h` of a (proper) top-level path :math:`P`
of :math:`e` is a prefix of the path that appears elsewhere (excluding
detached subqueries) in :math:`e`, such that the occurrences of
:math:`P_h` in :math:`e` do not coincide with the occurrences of
:math:`P_h` extended with the next path component in :math:`P`. In the
second example above, ``Person`` is a hoisting prefix of
``Person.first_name`` as it appears in ``Person.last_name``.

A Method of Computing Hoisting Prefixes
-------------------------------------------

Let :math:`toppath(e)` denote the list of all paths appearing in the
(proper) top-level of :math:`e` and their hoisting prefixes, without
duplicates, ordered lexicographically, and let :math:`n` denote the
length of the list, :math:`toppath(e)` may be computed as follows:

Let the symbol :math:`\looparrowright` denote either :math:`\cdot`,
:math:`\mathop{\operatorname{\mathtt{\cdot_{\leftarrow}}}}`, or
:math:`@`. The common longest path prefix (function :math:`clpp`)
between path :math:`R\looparrowright_1l_1\dots \looparrowright_nl_n` and
:math:`R'\looparrowright_1' l_1'\dots \looparrowright_{m}'l_{m}'` is the
longest path that is the prefix of both path and is undefined/empty
otherwise. :math:`R` is a variable or a name.

Let the longest common prefix set (function :math:`clpps`) of a set of
path :math:`S` be the set of nonempty prefix paths any two paths of set
:math:`S` :math:`clpps(S) = \{clpp(s, t) : \forall s, t \in S\}`. It
will be the case that :math:`S \subseteq clpps(S)`. We also define
separate common longest prefix set
:math:`dclpps(A, B) = \{clpp(s, t) : \forall s \in A, \forall t \in B\}`.

#. Let :math:`A` be the set of all paths appearing in the top-level of
   :math:`e`.

#. Let :math:`B` be the set of all paths appearing in :math:`e` and all
   its subqueries but not those in a detached expression.

#. For each :math:`b_i \in B`, let :math:`C_i` be the set of common
   longest path prefixes among :math:`b_i` and :math:`A`, i.e.,
   :math:`C_i = dclpps(A, \{b_i\})`

#. :math:`toppath(E)` will be :math:`A` union the all the
   :math:`C_i`\ ’s, deduplicated and sorted, i.e.,
   :math:`toppath(E) = \mathop{\operatorname{\mathit{{sorted}}}}(\mathop{\operatorname{\mathit{{distinct}}}}(clpps(A) \cup (\cup_i C_i)))`.

The Hoisting Process
--------------------

Given an expression :math:`e`, recall that :math:`toppath(e)` denotes
the list of all paths appearing in the (proper) top-level of :math:`e`
and their hoisting prefixes, without duplicates, ordered
lexicographically. Let :math:`n` denote the length of the
:math:`toppath(e)`, and let :math:`P_i` denote the :math:`i`\ th path in
:math:`toppath(e)`,

let :math:`\mathop{\operatorname{\mathit{{select\_hoist}}}}` be the
function that hoists both top-level and sublevel queries, and the
function :math:`\mathop{\operatorname{\mathit{{sub\_select\_hoist}}}}`
is the function that calls
:math:`\mathop{\operatorname{\mathit{{select\_hoist}}}}` on all
subqueries and semi-subqueries. Then,

.. math::

   \begin{aligned}
   \mathop{\operatorname{\mathit{{select\_hoist}}}}(e) 
   & = \operatorname{\mathsf{{optional\_for}}}(Y_1 \leftarrow P_1; 
   \\& \operatorname{\mathsf{{optional\_for}}}(Y_2 \leftarrow <Y_1/P_1>P_2;
   \\& \operatorname{\mathsf{{optional\_for}}}(Y_3 \leftarrow <Y_1/P_1><Y_2/P_2>P_3;
   \\& \dots 
   \\& \operatorname{\mathsf{{optional\_for}}}(Y_n \leftarrow <Y_1/P_1>\dots <Y_{n-1}/P_{n-1}>P_n;\\
   & \mathop{\operatorname{\mathit{{sub\_select\_hoist}}}}(<Y_1/P_1> \dots <Y_n/P_n>e)
   ) \dots )))\end{aligned}

where :math:`Y_i`\ ’s are fresh.

The above query doesn’t quite work when
:math:`e = e_1\, \mathop{\operatorname{\mathit{{filter}}}}\, (x.e_2)\, \mathop{\operatorname{\mathit{{order}}}}\, (x.e_3)`
as the hoisted for on the outside will break the aggregate nature of the
:math:`\mathop{\operatorname{\mathit{{order}}}}` function. Thus, we have
the following workaround:

.. math::

   \begin{aligned}
   \mathop{\operatorname{\mathit{{select\_hoist}}}}&(e = e_1 \, \mathop{\operatorname{\mathit{{filter}}}}\, (x.e_2)\, \mathop{\operatorname{\mathit{{order}}}}\, (x.e_3)) 
    = 
   \\& ((\operatorname{\mathsf{{optional\_for}}}(Y_1 \leftarrow P_1; 
   \\& \operatorname{\mathsf{{optional\_for}}}(Y_2 \leftarrow <Y_1/P_1>P_2;
   \\& \operatorname{\mathsf{{optional\_for}}}(Y_3 \leftarrow <Y_1/P_1><Y_2/P_2>P_3;
   \\& \dots 
   \\& \operatorname{\mathsf{{optional\_for}}}(Y_n \leftarrow <Y_1/P_1>\dots <Y_{n-1}/P_{n-1}>P_n;\\
    & \operatorname{\mathsf{{with}}}(subject := \mathop{\operatorname{\mathit{{sub\_select\_hoist}}}}(<Y_i/P_i>_{1\le i\le n}e_1)\\
    &  \hspace{2em}\, \mathop{\operatorname{\mathit{{filter}}}}\, ({select\_hoist}(<Y_i/P_i>_{1\le i\le n}(x.e_2)))\, \mathop{\operatorname{\mathit{{order}}}}\, \{\});  \\
    & \{subject := subject, \\
    & \hspace{2em} order := ({select\_hoist}(<Y_i/P_i>_{1\le i\le n}([subject/x]e_3)))\}
    )
   \\
   & ))
   ) \dots ))))\,\mathop{\operatorname{\mathit{{filter}}}}\, (x.\mathop{\operatorname{\mathit{{True}}}})\, \mathop{\operatorname{\mathit{{order}}}}\, (x.x\cdot order) )\cdot subject\end{aligned}

:math:`<Y_i/_i>_{1\le i\le n}` is a shorthand for
:math:`<Y_1/P_1> \dots <Y_n/P_n>`

The essential idea is that we pack the ordering clause into an object
when executing the content of select and filter. Then we wrap the entire
for clauses into an outisde ordering and finally project out the
subject.

The path substitution never acts across
:math:`\mathop{\operatorname{\mathit{{detached}}}}` queries. Detached
queries do not participate in path factoring.

.. _`sec:optimization`:

IR Oprimizations
================

This section describes some common transformation that may be used in
the query optimizer of the |image|\ **EdgeQL**\ IR described in the
previous section.

The transformation is denoted by the judgment :math:`e\mapsto e'`.

Commuting Conversion of For Loops
---------------------------------

.. math::

   \operatorname{\mathsf{{for}}}(x \leftarrow \operatorname{\mathsf{{for}}}(y \leftarrow e_2; e_3); e_1) 
   \mapsto \operatorname{\mathsf{{for}}}(y \leftarrow e_2; \operatorname{\mathsf{{for}}}(x \leftarrow e_3; e_1))

provided :math:`y \notin FV(e_1)` which is always possible by
:math:`\alpha`-renamings.

Examples
========

Select
------

The query below :

::

   select Movie {
       title, 
       actors : {
           name
       },
       title_length := len(title)
   } 
   filter Movie.title_length > 10
   order by Movie.title
   limit 2

In the static context:

:math:`\Delta := Movie : \{title : str^{(=1)}, actors : Actor ^{(*)}\}, Actors : \{name : str^{(=1)}\}`

Elaborated Expression (Without Path Factoring):

::

   select Movie {
               title := .title,
               actors := .actors {
                   name := .name
               },
               title_length := std::len(title)
           }
   filter .title_length > 10
   order by .title ASC
   offset 0
   limit 2

After Path Factoring:

::

   select
       ((select
           (for optional n494 in Movie
           union 
               (for optional n495 in (select
                   n494 {
                       title := (for optional n497 in .title union n497),
                       actors := (for optional n499 in .actors
                       union 
                           n499 {
                               name := (for optional n501 in .name union n501)
                           }),
                       title_length := (for optional n503 in title 
                                        union std::len(n503))
                   }
               filter
                   (for optional n505 in .title_length union (n505 > 10))
               )
               union 
                   {
                       subject := n495,
                       order := {
                           `0-ascending` := (for optional n507 in n495.title
                                             union n507)
                       }
                   }))
       order by .order
       )).subject
   offset 0
   limit 2

.. _`sec:concrete_syntax`:

Concrete Syntax
===============

We will present the full surface syntax of |image|\ **EdgeQL**\ in this
section, and then in the next section, we will present the elaboration
from the surface syntax into the IR syntax of Section 1.

We have the following syntactic categories:

Names, Shapes, Expressions

:math:`N, S, E`

Names
-----

Names can be fully-qualified or unqualified, and both are represented by
the syntactic class :math:`N`, we use the lower case :math:`n` to mean
single name.

Shapes
------

.. math:: S ::= \{C_1, \dots, C_n\}

[index:Shapes]\ *Shapes* are a list of shape elements :math:`se`.

.. math:: C ::= L \mid L := E \mid l : S

[index:Shape elements]\ *Shape elements* are names, computed properties,
computed link properties

.. _expressions-1:

Expressions
-----------

.. math:: E ::=  v \mid S \mid E\ S

.. math:: \mid  select\, E\, \mathop{\operatorname{\mathit{{filter}}}}\, E_2\, \mathop{\operatorname{\mathit{{order}}}}\, E_3\, \mathop{\operatorname{\mathit{{offset}}}}\, E_4 \, \mathop{\operatorname{\mathit{{limit}}}}\, E_5

.. math:: \mid insert\, N\, S

.. math:: \mid update\, E\, set\, S

.. math:: \mid group\, E\, S\, by\, E_1, \dots, E_n

.. math:: \mid \mathop{\operatorname{\mathit{{for}}}}\, X \, in\, E_1\, union\, E_2 \mid \mathop{\operatorname{\mathit{{with}}}}\, X := E_1\, E_2

.. math::

   \mid (l_1 := E_1, \dots, l_n := E_n) \mid (E_1, \dots, E_n) \mid [E_1, \dots, E_n]
       \mid \{E_1, \dots, E_n\}

Expressions include

primitive values,

shapes, expression with shapes

selects (with optional clauses, the default values are :
:math:`S = \{\}`, :math:`E_2 = \mathop{\operatorname{\mathit{{true}}}}`,
:math:`E_3 = ()`, :math:`E_4 = 0`, :math:`E_5 = \infty`)

inserts with shapes (optional, default : :math:`\{\}`)

updates with shapes

groups

for loops, with bindings

named and unnamed tuples, arrays, multiset constructions

.. _`sec:elaboration`:

Elaboration
===========

We use the judgment :math:`\Delta; \Gamma \vdash E \rightsquigarrow e`
to mean that surface query :math:`E` elaborates to the IR expression
:math:`e` under schema :math:`\Delta` and variable context
:math:`\Gamma`.

We omit :math:`\Delta; \Gamma \vdash` if it stays unchanged in the
premises and the conclusion.

Elaboration of Shapes and Shape Components
------------------------------------------

The elaboration is a direct-style structural recursion.

**Shapes.** :math:`S \rightsquigarrow s`

.. math::

   \inferrule{ 
           \forall i. C_i \rightsquigarrow c_i
       }{ 
           \{C_1, \dots, C_n\} \rightsquigarrow\{c_1, \dots, c_n\}
       }

**Shape Components.** :math:`C \rightsquigarrow c`

#. 

   .. math:: \inferrule{ }{ L \rightsquigarrow L}

   Without implicit syntax sugar:

   .. math:: \inferrule{ }{ l \rightsquigarrow l := x.(x \cdot l)}

   .. math:: \inferrule{ }{ @l \rightsquigarrow@l := x.(x \mathop{\operatorname{\mathtt{{@}}}}l)}

#. 

   .. math:: \inferrule{ S \rightsquigarrow s }{ l : S \rightsquigarrow l : s}

   Without implicit syntax sugar:

   .. math:: \inferrule{ S \rightsquigarrow s }{ l : S \rightsquigarrow l := x.((x\cdot l)\, s)}

#. 

   .. math:: \inferrule{ E \rightsquigarrow e }{ L := E \rightsquigarrow L := \P. e}

   We use the symbol :math:`\P` as the symbol that we put uniformly
   before the leading dot in a path. We create abstraction over this
   symbol here.

   Note order and limit clauses attached to shapes and shape components
   can be directly elaborated into selects with order and filter:
   :math:`L := x. E \, \mathop{\operatorname{\mathit{{filter}}}}\, E_1 \, \mathop{\operatorname{\mathit{{order\, by}}}}\, E_2`
   is elaborated into
   :math:`L := x. e\, \mathop{\operatorname{\mathit{{filter}}}}\, e_2\, \mathop{\operatorname{\mathit{{order}}}}\, e_3`.

Elaboration Rules
-----------------

#. Primitive Values

   .. math::

      \inferrule{
              }{
                  v \rightsquigarrow v
              }

#. Inserts

   .. math::

      \inferrule{
                  \Delta, N := \tau, \Gamma \vdash  S \rightsquigarrow s \\
                  \mathop{\operatorname{\mathit{{shape\_to\_obj(s) = e}}}}
              }{
                  \Delta, N := \tau, \Gamma \vdash insert\, N\, S \rightsquigarrow insert\, N\, e
              }

#. Selects

   .. math::

      \inferrule{
                  \forall i. E_i \rightsquigarrow e_i
              }{
                  \mathop{\operatorname{\mathit{{select}}}} \, E_1\, \mathop{\operatorname{\mathit{{filter}}}}\, E_2\, \mathop{\operatorname{\mathit{{order}}}}\, E_3\, \mathop{\operatorname{\mathit{{limit}}}}\, E_4\, \mathop{\operatorname{\mathit{{offset}}}}\, E_5
                  \\
                  \rightsquigarrow
                  \mathop{\operatorname{\mathit{{select}}}}\, ((\mathop{\operatorname{\mathit{{select}}}}\, (e_1\, \mathop{\operatorname{\mathit{{filter}}}}\, (x.e_2)\, \mathop{\operatorname{\mathit{{order}}}}\, (x.e_3)))\, \mathop{\operatorname{\mathit{{offset}}}}\, e_4 \, \mathop{\operatorname{\mathit{{limit}}}}\, e_5)
              }

   Special rule of select
   Elaboration\ https://edgedb.slack.com/archives/C04JG7CR04T/p1677711136147779

#. Updates

   .. math::

      \inferrule{
                   E \rightsquigarrow e \\
                   S \rightsquigarrow s
              }{
                  \mathop{\operatorname{\mathit{{update}}}}\, E\, \mathop{\operatorname{\mathit{{set}}}} \, S 
                  \rightsquigarrow
                  \mathop{\operatorname{\mathit{{update}}}}\, e\, s
              }

A List of Syntactic Classes
===========================

.. container:: multicols

   2

   #. :math:`c` - `cardinality mode <#index:cardinality mode>`__, `shape
      components <#index:shape components>`__

   #. :math:`C` - `Shape elements <#index:Shape elements>`__

   #. :math:`D` - `implementation-dependent
      data <#index:implementation-dependent data>`__

   #. :math:`e` - `expressions <#index:expressions>`__

   #. :math:`i` - `cardinal number <#index:cardinal number>`__

   #. :math:`l` - `primitive label
      names <#index:primitive label names>`__

   #. :math:`L` - `label <#index:label>`__

   #. :math:`\mu` - `database storage <#index:database storage>`__

   #. :math:`m` - `cardinality and multiplicity
      mode <#index:cardinality and multiplicity mode>`__

   #. :math:`M` - `modified schema type
      component <#index:modified schema type component>`__

   #. :math:`N` - `table names <#index:table names>`__

   #. :math:`p` - `parameter modifier <#index:parameter modifier>`__

   #. :math:`P` - `path <#index:path>`__

   #. :math:`s` - `shape <#index:shape>`__

   #. :math:`S` - `Shapes <#index:Shapes>`__, `runtime
      snapshot <#index:runtime snapshot>`__

   #. :math:`\tau` - `types <#index:types>`__

   #. :math:`T` - `schema types <#index:schema types>`__

   #. :math:`t` - `schema type
      components <#index:schema type components>`__

   #. :math:`u` - `property marker <#index:property marker>`__

   #. :math:`U` - `set of values <#index:set of values>`__

   #. :math:`v` - `primitive values <#index:primitive values>`__

   #. :math:`V` - `values <#index:values>`__

   #. :math:`W` - `objects <#index:objects>`__

Types of Builtin Functions
==========================

Builtin Binary Operations
-------------------------

#. ``+`` (Addition)

   :math:`: [int^1 , int^1] \to int^{(=1)}`

#. ``-`` (Subtraction)

   :math:`: [int^1 , int^1] \to int^{(=1)}`

#. ``*`` (Multiplication)

   :math:`: [int^1 , int^1] \to int^{(=1)}`

   TODO: Polymorphism? Floats?

#. ``%`` (Modulo)

   :math:`: [int^1 , int^1] \to int^{(=1)}`

#. ``=`` (Equal)

   :math:`: [some_0^1 , some_0^1] \to bool^{(=1)}`

#. ``!=`` (Not Equal)

   :math:`: [some_0^1 , some_0^1] \to bool^{(=1)}`

#. ``?=`` (Optional Equal)

   :math:`: [some_0^? , some_0^?] \to bool^{(=1)}`

#. ``?!=`` (Optional Not Equal)

   :math:`: [some_0^? , some_0^?] \to bool^{(=1)}`

#. ``>`` (Greater Than)

   :math:`: [some_0^1 , some_0^1] \to bool^{(=1)}`

#. ``++`` (Concatenate)

   :math:`: [str^1 , str^1] \to str^{(=1)}`

   :math:`: [\operatorname{\mathsf{{arr}}}(some_0)^1 , \operatorname{\mathsf{{arr}}}(some_0)^1] \to \operatorname{\mathsf{{arr}}}(some_0)^{(=1)}`

#. ``??`` (Coalescing)

   :math:`: [some_0^{?}, some_0^{*}] \to some_0^{(*)}`

   \* Special Cardinality Inference

   If the input arguments have cardinalities :math:`[i_1,i_2],i_3` and
   :math:`[i_4, i_5], i_6` respectively, then the output cardinality is
   :math:`[min(i_1,i_4), max(i_2,i_5)],max(i_3,i_6)`.

#. ``IN``

   :math:`: [some_0^1 , some_0^*] \to bool^{(=1)}`

#. ``EXISTS``

   :math:`: [any^* ] \to bool^{(=1)}`

#. ``OR``

   :math:`: [bool^1 , bool^1] \to bool^{(=1)}`

Reserved Operations
-------------------

Some implementation tricks for language constructs:

#. ``_[_]`` Indexing

   :math:`: [str ^1, int ^ 1] \to str^{(=1)}`

   :math:`: [\operatorname{\mathsf{{arr}}}(some_0) ^1, int ^ 1] \to \operatorname{\mathsf{{arr}}}(some_0)^{(=1)}`

#. ``_[_:_]`` Slicing with starts and stops

   :math:`: [str ^1, int ^ 1, int^1_\infty] \to str^{(=1)}`

   :math:`: [\operatorname{\mathsf{{arr}}}(some_0) ^1, int ^ 1, int ^ 1_\infty] \to \operatorname{\mathsf{{arr}}}(some_0)^{(=1)}`

Standard Library Functions
--------------------------

Some standard library function:

#. ``std::any`` :math:`: [ bool ^*] \to  bool^{(=1)}`

#. ``std::array_agg``
   :math:`: [some_0 ^*] \to \operatorname{\mathsf{{arr}}}( some_0)^{(=1)}`

#. ``std::array_unpack``
   :math:`: [\operatorname{\mathsf{{arr}}}(some_0) ^1] \to  some_0^{(*)}`

#. ``std::count`` :math:`: [ any ^*] \to  int^{(=1)}`

#. ``std::enumerate``
   :math:`: [some_0 ^*] \to \operatorname{\mathsf{{prod}}}(int, some_0)^{(*)}`

#. ``std::len``

   :math:`: [str^1] \to int ^{(=1)}`

   :math:`: [\operatorname{\mathsf{{arr}}}(any)^1] \to int ^{(=1)}`

#. ``std::sum`` :math:`: [int ^*] \to int^{(=1)}`

:raw-latex:`\cite{Harper16book}`

.. |image| image:: images/edgeql-logo-opaque.eps
