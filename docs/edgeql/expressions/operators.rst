.. _ref_eql_ops:

=========
Operators
=========

EdgeDB provides a number of operators which can be used to build
expressions. They can be broken down into several categories:


Equality Operators
------------------

These are :eql:op:`= <EQ>`, :eql:op:`!= <NEQ>` and their counter-parts
that handle ``{}`` in a special way: :eql:op:`?= <COALEQ>`,
:eql:op:`?!= <COALNEQ>`. They handle identity comparisons.


Ordering Operators
------------------

The :eql:op:`\< <LT>`, :eql:op:`\> <GT>`, :eql:op:`\<= <LTEQ>`,
:eql:op:`\>= <GTEQ>` handle comparisons of relative value. In addition
to be applicable to numbers, these operators can be applied to any
type of values in EdgeDB, because everything is considered to have
some implicit ordering.


Boolean Operators
-----------------

The standard boolean operators :eql:op:`AND`, :eql:op:`OR`, and
:eql:op:`NOT` are available for expressing various conditions and
``FILTER`` expressions.


Arithmetic Operators
--------------------

Standard math operators include: :eql:op:`+ <PLUS>`, :eql:op:`-
<MINUS>`, :eql:op:`* <MULT>`, :eql:op:`/ <DIV>`. There are also the
unary minus :eql:op:`- <UMINUS>`, the floor division :eql:op:`//
anyreal <FLOORDIV>`, the modulo :eql:op:`% anyreal <MOD>`, and the
power :eql:op:`^ anyreal <POW>`.


Indexing and Slicing Operators
------------------------------

:ref:`Arrays <ref_std_array>`, :ref:`strings <ref_std_string>` and
:ref:`bytes <ref_std_bytes>` all support indexing :eql:op:`[i]
<STRIDX>`, slicing :eql:op:`[from:to] <STRSLICE>`, and
concatenation :eql:op:`++ <STRPLUS>`.

:ref:`JSON <ref_std_json>` arrays, strings, and objects
support indexing :eql:op:`[i] <JSONIDX>` and
slicing :eql:op:`[from:to] <JSONSLICE>`.


Set Operators
-------------

These work with whole sets and include: :eql:op:`DISTINCT`,
:eql:op:`EXISTS`, :eql:op:`UNION`, and :eql:op:`IN`. There's also less
obvious ones such as the ternary :eql:op:`IF ... ELSE <IF..ELSE>`,
the coalesce operator :eql:op:`?? <COALESCE>`, and the type
intersection operator :eql:op:`[IS type] <ISINTERSECT>`.


Type Operators
--------------

The most common type operator is casting :eql:op:`\<type\> <CAST>`. The
other ones can be useful for introspection: :eql:op:`IS <IS>`,
:eql:op:`TYPEOF <TYPEOF>`, :eql:op:`INTROSPECT <INTROSPECT>`.
