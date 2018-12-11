.. _ref_eql_lexical:


Lexical Structure
=================

Every EdgeQL command is composed of a sequence of *tokens*, terminated by
a semicolon (``;``).  The types of valid tokens as well as their order
is determined by the syntax of the particular command.

EdgeQL is case sensistive except for *keywords* (in the examples the
keywords are written in upper case as a matter of convention).

There are several kinds of tokens: *keywords*, *identifiers*,
*literals* (constants) and *symbols* (operators and punctuation).

Tokens are normally separated by whitespace (space, tab, newline) or
comments.


Identifiers
-----------

There are two ways of writing identifiers in EdgeQL: plain and quoted.
The plain identifiers are similar to many other languages, they are
alphanumeric with underscores and cannot start with a digit. The
quoted identifiers start and end with a *backtick*
```quoted.identifier``` and can contain any characters inside, but
must not start with an ampersand (``@``).

.. productionlist:: edgeql
    identifier: ``plain_ident`` | ``quoted_ident``
    plain_ident: ``ident_first`` `ident_rest`*
    ident_first: <any letter, underscore>
    ident_rest: <any letter, digits, underscore>
    quoted_ident: "`" ``qident_first`` `qident_rest`* "`"
    qident_first: <any character except "@">
    qident_rest: <any character>

Quoted identifiers usually needed to represent module names that
contain a dot (``.``) or to distinguish *names* from *reserved keywords*
(for instance to allow referring to a link named "order" as ```order```).


Names and keywords
------------------

.. TODO::

    This section needs a significant update.

There are a number of *reserved* and *unreserved* keywords in EdgeQL.
Every identifier that is not a *reserved* keyword is a valid *name*.
*Names* are used to refer to concepts, links, link properties, etc.

.. productionlist:: edgeql
    short_name: ``not_keyword_ident`` | ``quoted_ident``
    not_keyword_ident: <any ``plain_ident`` except for `keyword`>
    keyword: ``reserved_keyword`` | ``unreserved_keyword``
    reserved_keyword: case insensitive sequence matching any
                    : of the following
                    : "AGGREGATE" | "ALTER" | "AND" |
                    : "ANY" | "COMMIT" | "CREATE" |
                    : "DELETE" | "DETACHED" | "DISTINCT" |
                    : "DROP" | "ELSE" | "EMPTY" | "EXISTS" |
                    : "FALSE" | "FILTER" | "FUNCTION" |
                    : "GET" | "GROUP" | "IF" | "ILIKE" |
                    : "IN" | "INSERT" | "IS" | "LIKE" |
                    : "LIMIT" | "MODULE" | "NOT" | "OFFSET" |
                    : "OR" | "ORDER" | "OVER" |
                    : "PARTITION" | "ROLLBACK" | "SELECT" |
                    : "SET" | "SINGLETON" | "START" | "TRUE" |
                    : "UPDATE" | "UNION" | "WITH"
    unreserved_keyword: case insensitive sequence matching any
                      : of the following
                      : "ABSTRACT" | "ACTION" | "AFTER" |
                      : "ARRAY" | "AS" | "ASC" | "ATOM" |
                      : "ATTRIBUTE" | "BEFORE" | "BY" |
                      : "CONCEPT" | "CONSTRAINT" |
                      : "DATABASE" | "DESC" | "EVENT" |
                      : "EXTENDING" | "FINAL" | "FIRST" |
                      : "FOR" | "FROM" | "INDEX" |
                      : "INITIAL" | "LAST" | "LINK" |
                      : "MAP" | "MIGRATION" | "OF" | "ON" |
                      : "POLICY" | "PROPERTY" |
                      : "REQUIRED" | "RENAME" | "TARGET" |
                      : "THEN" | "TO" | "TRANSACTION" |
                      : "TUPLE" | "VALUE" | "VIEW"

Fully-qualified names consist of a module, ``::``, and a short name.
They can be used in most places where a short name can appear (such as
paths and shapes).

.. productionlist:: edgeql
    name: ``short_name`` | ``fq_name``
    fq_name: ``short_name`` "::" ``short_name`` |
           : ``short_name`` "::" ``unreserved_keyword``


.. _ref_eql_lexical_const:

Constants
---------

.. TODO


.. _ref_eql_lexical_dollar_quoting:

Dollar-quoted String Constants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Operators
---------

.. TODO


Punctuation
-----------

.. TODO


Comments
--------

.. TODO


Operator Precedence
-------------------

EdgeQL operators listed in order of precedence from lowest to highest:

.. list-table::
    :widths: auto
    :header-rows: 1

    * - operator
    * - :eql:op:`UNION`
    * - :eql:op:`IF..ELSE`
    * - :eql:op:`OR`
    * - :eql:op:`AND`
    * - :eql:op:`NOT`
    * - :eql:op:`=<EQ>`, :eql:op:`\!=<NEQ>`, :eql:op:`?=<COALEQ>`,
        :eql:op:`?\!=<COALNEQ>`
    * - :eql:op:`\<<LT>`, :eql:op:`><GT>`, :eql:op:`\<=<LTEQ>`,
        :eql:op:`>=<GTEQ>`
    * - :eql:op:`LIKE`, :eql:op:`ILIKE`
    * - :eql:op:`IN`, :eql:op:`NOT IN <IN>`
    * - :eql:op:`IS`, :eql:op:`IS NOT <IS>`
    * - :eql:op:`+<PLUS>`, :eql:op:`-<MINUS>`
    * - :eql:op:`/<DIV>`, :eql:op:`*<MULT>`, :eql:op:`%<MOD>`
    * - :eql:op:`??<COALESCE>`
    * - :eql:op:`DISTINCT`, unary :eql:op:`-<UMINUS>`
    * - :eql:op:`^<POW>`
    * - :ref:`type cast <ref_eql_expr_typecast>`
