.. _ref_edgeql_syntax:


Syntax
======

EdgeQL is a declarative language and a large part of its expressive
power lies in the structures that describe the data. It is white space
insensitive, using ``;`` as a statement terminator. It is case
sensitive except for *keywords* (in the examples the keywords are
written in upper case as a matter of convention).


Identifiers
-----------

There are two ways of writing identifiers in EdgeQL: plain and quoted.
The plain identifiers are similar to many other languages, they are
alphanumeric with underscores and cannot start with a digit. The
quoted identifiers start and end with a *backtick*
```quoted.identifier``` and can contain any characters inside, but
must not start with an "@".

.. productionlist:: edgeql
    identifier: `plain_ident` | `quoted_ident`
    plain_ident: `ident_first` `ident_rest`*
    ident_first: <any letter, underscore>
    ident_rest: <any letter, digits, underscore>
    quoted_ident: "`" `qident_first` `qident_rest`* "`"
    qident_first: <any character except "@">
    qident_rest: <any character>

Quoted identifiers usually needed to represent module names that
contain "." or to distinguish *names* from *keywords* (for instance to
allow referring to a link named "order" as ```order```).


Names and keywords
------------------

There are a number of *reserved* and *unreserved* keywords in EdgeQL.
Every identifier that is not a *reserved* keyword is a valid *name*.
*Names* are used to refer to concepts, links, link properties, etc.

.. productionlist:: edgeql
    short_name: `not_keyword_ident` | `quoted_ident`
    not_keyword_ident: <any `plain_ident` except for `keyword`>
    keyword: `reserved_keyword` | `unreserved_keyword`
    reserved_keyword: case insensitive sequence matching any
                    : of the following
                    : "AGGREGATE" | "ALL" | "ALTER" | "AND" |
                    : "ANY" | "COMMIT" | "CREATE" |
                    : "DELETE" | "DISTINCT" | "DROP" |
                    : "ELSE" | "EMPTY" | "EXISTS" | "FALSE" |
                    : "FILTER" | "FUNCTION" | "GET" |
                    : "GROUP" | "IF" | "ILIKE" | "IN" |
                    : "INSERT" | "IS" | "LIKE" | "LIMIT" |
                    : "MODULE" | "NOT" | "OFFSET" | "OR" |
                    : "ORDER" | "OVER" | "PARTITION" |
                    : "ROLLBACK" | "SELECT" |
                    : "SET" | "SINGLETON" | "START" | "TRUE" |
                    : "UPDATE" | "UNION" | "WITH"
    unreserved_keyword: case insensitive sequence matching any
                      : of the following
                      : "ABSTRACT" | "ACTION" | "AFTER" | "ARRAY" |
                      : "AS" | "ASC" | "ATOM" | "ATTRIBUTE" | "BEFORE" |
                      : "BY" | "CONCEPT" | "CONSTRAINT" | "DATABASE" |
                      : "DESC" | "EVENT" | "EXTENDING" | "FINAL" |
                      : "FIRST" | "FOR" | "FROM" | "INDEX" | "INITIAL" |
                      : "LAST" | "LINK" | "MAP" | "MIGRATION" | "OF" |
                      : "ON" | "POLICY" | "PROPERTY" | "REQUIRED" |
                      : "RENAME" | "TARGET" | "THEN" | "TO" |
                      : "TRANSACTION" | "TUPLE" | "VALUE" | "VIEW"

Fully-qualified names consist of a module, ``::``, and a short name.
They can be used in most places where a short name can appear (such as
paths and shapes).

.. productionlist:: edgeql
    name: `short_name` | `fq_name`
    fq_name: `short_name` "::" `short_name` |
           : `short_name` "::" `unreserved_keyword`


Operators
---------

EdgeQL operators listed in order of precedence:

+------------------+-----------+-----------+-----------+----------+
| operator         | left      | middle    | right     | result   |
+==================+===========+===========+===========+==========+
| UNION ALL        | set of    | --        | set of    | set of   |
|                  | atoms     |           | atoms     | atoms    |
+------------------+-----------+-----------+-----------+----------+
| UNION            | set       | --        | set       | set      |
+------------------+-----------+-----------+-----------+----------+
| IF .. ELSE       | set       | bool      | set       | set      |
+------------------+-----------+-----------+-----------+----------+
| OR               | bool      | --        | bool      | bool     |
+------------------+-----------+-----------+-----------+----------+
| AND              | bool      | --        | bool      | bool     |
+------------------+-----------+-----------+-----------+----------+
| NOT              | --        | --        | bool      | bool     |
+------------------+-----------+-----------+-----------+----------+
| =, !=            | any       | --        | any       | bool     |
+------------------+-----------+-----------+-----------+----------+
| <, >, <=, >=     | any       | --        | any       | bool     |
+------------------+-----------+-----------+-----------+----------+
| LIKE, ILIKE      | str       | --        | str       | str      |
+------------------+-----------+-----------+-----------+----------+
| IN, NOT IN       | set       | --        | set       | set of   |
|                  |           |           |           | bool     |
+------------------+-----------+-----------+-----------+----------+
| IS, IS NOT       | any       | --        | Class,    | bool     |
|                  |           |           | sequence  |          |
|                  |           |           | of Classes|          |
+------------------+-----------+-----------+-----------+----------+
| +, -             | number    | --        | number    | number   |
+------------------+-----------+-----------+-----------+----------+
| \+               | str       | --        | str       | str      |
+------------------+-----------+-----------+-----------+----------+
| EXISTS           | --        | --        | set       | bool     |
+------------------+-----------+-----------+-----------+----------+
| \*, /, %         | number    | --        | number    | number   |
+------------------+-----------+-----------+-----------+----------+
| ??               | set       | --        | set       | set      |
+------------------+-----------+-----------+-----------+----------+
| DISTINCT         | --        | --        | set       | set      |
+------------------+-----------+-----------+-----------+----------+
| unary +, -       | --        | --        | number    | number   |
+------------------+-----------+-----------+-----------+----------+
| ^                | number    | --        | number    | number   |
+------------------+-----------+-----------+-----------+----------+

All set operators (``UNION ALL``, ``UNION``, ``EXISTS``, ``DISTINCT``,
``??``, ``IF..ELSE``, ``IN`` and ``NOT IN``) handle empty set ``{}``
as a normal valid input. All other operators when operating on ``{}``,
return ``{}``. For more details see
:ref:`how expressions work<ref_edgeql_expressions>`.
