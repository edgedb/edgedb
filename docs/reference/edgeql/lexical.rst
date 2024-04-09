.. _ref_eql_lexical:


Lexical structure
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
```quoted.identifier``` and can contain any characters inside with a
few exceptions. They must not start with an ampersand (``@``) or
contain a double colon (``::``). If there's a need to include a backtick
character as part of the identifier name a double-backtick sequence
(``````) should be used: ```quoted``identifier``` will result in the
actual identifier being ``quoted`identifier``.

.. productionlist:: edgeql
    identifier: `plain_ident` | `quoted_ident`
    plain_ident: `ident_first` `ident_rest`*
    ident_first: <any letter, underscore>
    ident_rest: <any letter, digits, underscore>
    quoted_ident: "`" `qident_first` `qident_rest`* "`"
    qident_first: <any character except "@">
    qident_rest: <any character>

Quoted identifiers are usually needed to represent module names that
contain a dot (``.``) or to distinguish *names* from *reserved keywords*
(for instance to allow referring to a link named "order" as ```order```).


.. _ref_eql_lexical_names:

Names and keywords
------------------

.. TODO::

    This section needs a significant update.

There are a number of *reserved* and *unreserved* keywords in EdgeQL.
Every identifier that is not a *reserved* keyword is a valid *name*.
*Names* are used to refer to concepts, links, link properties, etc.

.. TODO: update this for "branch"

.. productionlist:: edgeql
    short_name: `not_keyword_ident` | `quoted_ident`
    not_keyword_ident: <any `plain_ident` except for `keyword`>
    keyword: `reserved_keyword` | `unreserved_keyword`
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
                      : "ANNOTATION" | "BEFORE" | "BY" |
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
    name: `short_name` | `fq_name`
    fq_name: `short_name` "::" `short_name` |
           : `short_name` "::" `unreserved_keyword`


.. _ref_eql_lexical_const:

Constants
---------

A number of scalar types have literal constant expressions.


.. _ref_eql_lexical_str:

Strings
^^^^^^^

Production rules for :eql:type:`str` literals:

.. productionlist:: edgeql
    string: `str` | `raw_str`
    str: "'" `str_content`* "'" | '"' `str_content`* '"'
    raw_str: "r'" `raw_content`* "'" |
           : 'r"' `raw_content`* '"' |
           : `dollar_quote` `raw_content`* `dollar_quote`
    raw_content: <any character different from delimiting quote>
    dollar_quote: "$" `q_char0`? `q_char`* "$"
    q_char0: "A"..."Z" | "a"..."z" | "_"
    q_char: "A"..."Z" | "a"..."z" | "_" | "0"..."9"
    str_content: <newline> | `unicode` | `str_escapes`
    unicode: <any printable unicode character not preceded by "\">
    str_escapes: <see below for details>

The inclusion of "high ASCII" character in :token:`edgeql:q_char` in
practice reflects the ability to use some of the letters with
diacritics like ``ò`` or ``ü`` in the dollar-quote delimiter.

Here's a list of valid :token:`edgeql:str_escapes`:

.. _ref_eql_lexical_str_escapes:

+--------------------+---------------------------------------------+
| Escape Sequence    | Meaning                                     |
+====================+=============================================+
| ``\[newline]``     | Backslash and all whitespace up to next     |
|                    | non-whitespace character is ignored         |
+--------------------+---------------------------------------------+
| ``\\``             | Backslash (\\)                              |
+--------------------+---------------------------------------------+
| ``\'``             | Single quote (')                            |
+--------------------+---------------------------------------------+
| ``\"``             | Double quote (")                            |
+--------------------+---------------------------------------------+
| ``\b``             | ASCII backspace (``\x08``)                  |
+--------------------+---------------------------------------------+
| ``\f``             | ASCII form feed (``\x0C``)                  |
+--------------------+---------------------------------------------+
| ``\n``             | ASCII newline (``\x0A``)                    |
+--------------------+---------------------------------------------+
| ``\r``             | ASCII carriage return (``\x0D``)            |
+--------------------+---------------------------------------------+
| ``\t``             | ASCII tabulation (``\x09``)                 |
+--------------------+---------------------------------------------+
| ``\xhh``           | Character with hex value hh                 |
+--------------------+---------------------------------------------+
| ``\uhhhh``         | Character with 16-bit hex value hhhh        |
+--------------------+---------------------------------------------+
| ``\Uhhhhhhhh``     | Character with 32-bit hex value hhhhhhhh    |
+--------------------+---------------------------------------------+

Here's some examples of regular strings using escape sequences

.. code-block:: edgeql-repl

    db> select 'hello
    ... world';
    {'hello
    world'}

    db> select "hello\nworld";
    {'hello
    world'}

    db> select 'hello \
    ...         world';
    {'hello world'}

    db> select 'https://edgedb.com/\
    ...         docs/edgeql/lexical\
    ...         #constants';
    {'https://edgedb.com/docs/edgeql/lexical#constants'}

    db> select 'hello \\ world';
    {'hello \ world'}

    db> select 'hello \'world\'';
    {"hello 'world'"}

    db> select 'hello \x77orld';
    {'hello world'}

    db> select 'hello \u0077orld';
    {'hello world'}

.. _ref_eql_lexical_raw:

Raw strings don't have any specially interpreted symbols; they contain
all the symbols between the quotes exactly as typed.

.. code-block:: edgeql-repl

    db> select r'hello \\ world';
    {'hello \\ world'}

    db> select r'hello \
    ... world';
    {'hello \
     world'}

    db> select r'hello
    ... world';
    {'hello
     world'}

.. _ref_eql_lexical_dollar_quoting:

Dollar-quoted String Constants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A special case of raw strings are *dollar-quoted* strings. They allow
using either kind of quote symbols ``'`` or ``"`` as part of the
string content without the quotes terminating the string. In fact,
because the *dollar-quote* delimiter sequences can have arbitrary
alphanumeric additional fillers, it is always possible to surround any
content with *dollar-quotes* in an unambiguous manner:

.. code-block:: edgeql-repl

    db> select $$hello
    ... world$$;
    {'hello
    world'}

    db> select $$hello\nworld$$;
    {'hello\nworld'}

    db> select $$"hello" 'world'$$;
    {"\"hello\" 'world'"}

    db> select $a$hello$$world$$$a$;
    {'hello$$world$$'}

More specifically, a delimiter:

* Must start with an ASCII letter or underscore
* Has following characters that can be digits 0-9, underscores or
  ASCII letters

.. _ref_eql_lexical_bytes:

Bytes
^^^^^

Production rules for :eql:type:`bytes` literals:

.. productionlist:: edgeql
    bytes: "b'" `bytes_content`* "'" | 'b"' `bytes_content`* '"'
    bytes_content: <newline> | `ascii` | `bytes_escapes`
    ascii: <any printable ascii character not preceded by "\">
    bytes_escapes: <see below for details>

Here's a list of valid :token:`edgeql:bytes_escapes`:

.. _ref_eql_lexical_bytes_escapes:

+--------------------+---------------------------------------------+
| Escape Sequence    | Meaning                                     |
+====================+=============================================+
| ``\\``             | Backslash (\\)                              |
+--------------------+---------------------------------------------+
| ``\'``             | Single quote (')                            |
+--------------------+---------------------------------------------+
| ``\"``             | Double quote (")                            |
+--------------------+---------------------------------------------+
| ``\b``             | ASCII backspace (``\x08``)                  |
+--------------------+---------------------------------------------+
| ``\f``             | ASCII form feed (``\x0C``)                  |
+--------------------+---------------------------------------------+
| ``\n``             | ASCII newline (``\x0A``)                    |
+--------------------+---------------------------------------------+
| ``\r``             | ASCII carriage return (``\x0D``)            |
+--------------------+---------------------------------------------+
| ``\t``             | ASCII tabulation (``\x09``)                 |
+--------------------+---------------------------------------------+
| ``\xhh``           | Character with hex value hh                 |
+--------------------+---------------------------------------------+


Integers
^^^^^^^^

There are two kinds of integer constants: limited size
(:eql:type:`int64`) and unlimited size (:eql:type:`bigint`). Unlimited
size integer :eql:type:`bigint` literals are similar to a regular
integer literals with an ``n`` suffix. The production rules are as
follows:

.. productionlist:: edgeql
    bigint: `integer` "n"
    integer: "0" | `non_zero` `digit`*
    non_zero: "1"..."9"
    digit: "0"..."9"

By default all integer literals are interpreted as :eql:type:`int64`,
while an explicit cast can be used to convert them to :eql:type:`int16`
or :eql:type:`int32`:

.. code-block:: edgeql-repl

    db> select 0;
    {0}

    db> select 123;
    {123}

    db> select <int16>456;
    {456}

    db> select <int32>789;
    {789}

Examples of :eql:type:`bigint` literals:

.. code-block:: edgeql-repl

    db> select 123n;
    {123n}

    db> select 12345678901234567890n;
    {12345678901234567890n}


Real Numbers
^^^^^^^^^^^^

Just as for integers, there are two kinds of real number constants:
limited precision (:eql:type:`float64`) and unlimited precision
(:eql:type:`decimal`). The :eql:type:`decimal` constants have the same
lexical structure as :eql:type:`float64`, but with an ``n`` suffix:

.. productionlist:: edgeql
    decimal: `float` "n"
    float: `float_wo_dec` | `float_w_dec`
    float_wo_dec: `integer_part` `exp`
    float_w_dec: `integer_part` "." `decimal_part`? `exp`?
    integer_part: "0" | `non_zero` `digit`*
    decimal_part: `digit`+
    exp: "e" ("+" | "-")? `digit`+

By default all float literals are interpreted as :eql:type:`float64`,
while an explicit cast can be used to convert them to :eql:type:`float32`:

.. code-block:: edgeql-repl

    db> select 0.1;
    {0.1}

    db> select 12.3;
    {12.3}

    db> select 1e3;
    {1000.0}

    db> select 1.2e-3;
    {0.0012}

    db> select <float32>12.3;
    {12.3}

Examples of :eql:type:`decimal` literals:

.. code-block:: edgeql-repl

    db> select 12.3n;
    {12.3n}

    db> select 12345678901234567890.12345678901234567890n;
    {12345678901234567890.12345678901234567890n}

    db> select 12345678901234567890.12345678901234567890e-3n;
    {12345678901234567.89012345678901234567890n}


Punctuation
-----------

EdgeQL uses ``;`` as a statement separator. It is idempotent, so
multiple repetitions of ``;`` don't have any additional effect.


Comments
--------

Comments start with a ``#`` character that is not otherwise part of a
string literal and end at the end of line. Semantically, a comment is
equivalent to whitespace.

.. productionlist:: edgeql
    comment: "#" <any other characters until the end of line>


Operators
---------

EdgeQL operators listed in order of precedence from lowest to highest:

.. list-table::
    :widths: auto
    :header-rows: 1

    * - operator
    * - :eql:op:`union`
    * - :eql:op:`if..else`
    * - :eql:op:`or`
    * - :eql:op:`and`
    * - :eql:op:`not`
    * - :eql:op:`=<eq>`, :eql:op:`\!=<neq>`, :eql:op:`?=<coaleq>`,
        :eql:op:`?\!=<coalneq>`
    * - :eql:op:`\<<lt>`, :eql:op:`><gt>`, :eql:op:`\<=<lteq>`,
        :eql:op:`>=<gteq>`
    * - :eql:op:`like`, :eql:op:`ilike`
    * - :eql:op:`in`, :eql:op:`not in <in>`
    * - :eql:op:`is`, :eql:op:`is not <is>`
    * - :eql:op:`+<plus>`, :eql:op:`-<minus>`, :eql:op:`++<strplus>`
    * - :eql:op:`*<mult>`, :eql:op:`/<div>`,
        :eql:op:`//<floordiv>`, :eql:op:`%<mod>`
    * - :eql:op:`?? <coalesce>`
    * - :eql:op:`distinct`, unary :eql:op:`-<uminus>`
    * - :eql:op:`^<pow>`
    * - :eql:op:`type cast <cast>`
    * - :eql:op:`array[] <arrayidx>`,
        :eql:op:`str[] <stridx>`,
        :eql:op:`json[] <jsonidx>`,
        :eql:op:`bytes[] <bytesidx>`
    * - :eql:kw:`detached`
