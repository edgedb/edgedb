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
```quoted.identifier``` and can contain any characters inside with a
few exceptions. They must not start with an ampersand (``@``) or
contain double-colon (``::``). If there's a need to include a backtick
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

Quoted identifiers usually needed to represent module names that
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

Production rules for :eql:type:`str` literal:

.. productionlist:: edgeql
    string: <str> | <raw_str>
    str: "'" `str_content`* "'" | '"' `str_content`* '"'
    raw_str: "r'" `raw_content`* "'" |
           : 'r"' `raw_content`* '"' |
           : `dollar_quote` `raw_content`* `dollar_quote`
    raw_content: <any character different from delimiting quote>
    dollar_quote: "$" `q_char0` ? `q_char`* "$"
    q_char0: "A"..."Z" | "a"..."z" | "_"
    q_char: "A"..."Z" | "a"..."z" | "_" | "0"..."9"
    str_content: <newline> | `unicode` | `str_escapes`
    unicode: <any printable unicode character not preceded by "\">
    str_escapes: <see below for details>

The inclusion of "high ASCII" character in the :token:`q_char` in
practice reflects the ability to use some of the letters with
diacritics like ``ò`` or ``ü`` in the dollar-quote delimiter.

Here's a list of valid :token:`str_escapes`:

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

    db> SELECT 'hello
    ... world';
    {'hello
    world'}

    db> SELECT "hello\nworld";
    {'hello
    world'}

    db> SELECT 'hello \
    ...         world';
    {'hello world'}

    db> SELECT 'https://edgedb.com/\
    ...         docs/edgeql/lexical\
    ...         #constants';
    {'https://edgedb.com/docs/edgeql/lexical#constants'}

    db> SELECT 'hello \\ world';
    {'hello \ world'}

    db> SELECT 'hello \'world\'';
    {"hello 'world'"}

    db> SELECT 'hello \x77orld';
    {'hello world'}

    db> SELECT 'hello \u0077orld';
    {'hello world'}

.. _ref_eql_lexical_raw:

Raw strings don't have any specially interpreted symbols, they contain
all the symbols between the quotes exactly as typed.

.. code-block:: edgeql-repl

    db> SELECT r'hello \\ world';
    {'hello \\ world'}

    db> SELECT r'hello \
    ... world';
    {'hello \
     world'}

    db> SELECT r'hello
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

    db> SELECT $$hello
    ... world$$;
    {'hello
    world'}

    db> SELECT $$hello\nworld$$;
    {'hello\nworld'}

    db> SELECT $$"hello" 'world'$$;
    {"\"hello\" 'world'"}

    db> SELECT $a$hello$$world$$$a$;
    {'hello$$world$$'}

More specifically delimiter:

* Must start with an ASCII letter or underscore
* Following characters can be digits 0-9, underscore or ASCII letters

.. _ref_eql_lexical_bytes:

Bytes
^^^^^

Production rules for :eql:type:`bytes` literal:

.. productionlist:: edgeql
    bytes: "b'" `bytes_content`* "'" | 'b"' `bytes_content`* '"'
    bytes_content: <newline> | `ascii` | `bytes_escapes`
    ascii: <any printable ascii character not preceded by "\">
    bytes_escapes: <see below for details>

Here's a list of valid :token:`bytes_escapes`:

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
    * - :eql:op:`UNION`
    * - :eql:op:`IF..ELSE`
    * - :eql:op:`OR`
    * - :eql:op:`AND`
    * - :eql:op:`NOT`
    * - :eql:op:`=<EQ>`, |neq|_, :eql:op:`?=<COALEQ>`,
        :eql:op:`?\!=<COALNEQ>`
    * - :eql:op:`\<<LT>`, :eql:op:`><GT>`, :eql:op:`\<=<LTEQ>`,
        :eql:op:`>=<GTEQ>`
    * - :eql:op:`LIKE`, :eql:op:`ILIKE`
    * - :eql:op:`IN`, :eql:op:`NOT IN <IN>`
    * - :eql:op:`IS`, :eql:op:`IS NOT <IS>`
    * - :eql:op:`+<PLUS>`, :eql:op:`-<MINUS>`, :eql:op:`++<STRPLUS>`
    * - :eql:op:`*<MULT>`, :eql:op:`/<DIV>`,
        :eql:op:`//<FLOORDIV>`, :eql:op:`%<MOD>`
    * - :eql:op:`??<COALESCE>`
    * - :eql:op:`DISTINCT`, unary :eql:op:`-<UMINUS>`
    * - :eql:op:`^<POW>`
    * - :eql:op:`type cast <CAST>`
    * - :eql:op:`array[] <ARRAYIDX>`,
        :eql:op:`str[] <STRIDX>`,
        :eql:op:`json[] <JSONIDX>`,
        :eql:op:`bytes[] <BYTESIDX>`
    * - :ref:`DETACHED <ref_eql_with_detached>`

.. |neq| replace:: !=
.. _neq: ./funcops/generic#operator::NEQ
