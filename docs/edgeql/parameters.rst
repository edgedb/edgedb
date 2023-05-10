.. _ref_eql_params:

Parameters
==========

:edb-alt-title: Query Parameters

EdgeQL queries can reference parameters with ``$`` notation. The value of these
parameters are supplied externally.

.. code-block:: edgeql

  select <str>$var;
  select <int64>$a + <int64>$b;
  select BlogPost filter .id = <uuid>$blog_id;

Note that we provided an explicit type cast before the parameter. This is
required, as it enables EdgeDB to enforce the provided types at runtime.

.. versionadded:: 3.0

    Parameters can be named or unnamed tuples.

    .. code-block:: edgeql

        select <tuple<str, bool>>$var;
        select <optional tuple<str, bool>>$var;
        select <tuple<name: str, flag: bool>>$var;
        select <optional tuple<name: str, flag: bool>>$var;

Usage with clients
------------------

REPL
^^^^

When you include a parameter reference in an EdgeDB REPL, you'll be prompted
interactively to provide a value or values.

.. code-block:: edgeql-repl

  db> select 'I ❤️ ' ++ <str>$var ++ '!';
  Parameter <str>$var: EdgeDB
  {'I ❤️ EdgeDB!'}


Python
^^^^^^

.. code-block:: python

  await client.query(
      "select 'I ❤️ ' ++ <str>$var ++ '!';",
      var="lamp")

  await client.query(
      "select <datetime>$date;",
      date=datetime.today())

JavaScript
^^^^^^^^^^

.. code-block:: javascript

  await client.query("select 'I ❤️ ' ++ <str>$name ++ '!';", {
    name: "rock and roll"
  });

  await client.query("select <datetime>$date;", {
    date: new Date()
  });

Go
^^

.. code-block:: go

  var result string
  err = db.QuerySingle(ctx,
    `select 'I ❤️ ' ++ <str>$var ++ '!';"`,
    &result, "Golang")

  var date time.Time
  err = db.QuerySingle(ctx,
    `select <datetime>$date;`,
    &date, time.Now())


Refer to the Datatypes page of your preferred :ref:`client library
<ref_clients_index>` to learn more about mapping between EdgeDB types and
language-native types.


Parameter types and JSON
------------------------

Prior to EdgeDB 3.0, parameters can be only :ref:`scalars
<ref_datamodel_scalar_types>` or arrays of scalars. In EdgeDB 3.0, parameters
can also be tuples. This may seem limiting at first, but in actuality this
doesn't impose any practical limitation on what can be parameterized. To pass
complex structures as parameters, use EdgeDB's built-in :ref:`JSON
<ref_std_json>` functionality.

.. code-block:: edgeql-repl

  db> with data := <json>$data
  ... insert Movie {
  ...   title := <str>data['title'],
  ...   release_year := <int64>data['release_year'],
  ... };
  Parameter <json>$data: {"title": "The Marvels", "release_year": 2023}
  {default::Movie {id: 8d286cfe-3c0a-11ec-aa68-3f3076ebd97f}}

Arrays can be "unpacked" into sets and assigned to ``multi`` links or
properties.

.. code-block:: edgeql

   with friends := (
     select User filter .id in array_unpack(<array<uuid>>$friend_ids)
   )
   insert User {
     name := <str>$name,
     friends := friends,
   };


Optional parameters
-------------------

By default, query parameters are ``required``; the query will fail if the
parameter value is an empty set. You can use an ``optional`` modifier inside
the type cast if the parameter is optional.

.. code-block:: edgeql-repl

  db> select <optional str>$name;
  Parameter <str>$name (Ctrl+D for empty set `{}`):
  {}

When using a client library, pass the idiomatic null pointer for your language:
``null``, ``None``, ``nil``, etc.

.. note::

  The ``<required foo>`` type cast is also valid (though redundant) syntax.

  .. code-block:: edgeql

    select <required str>$name;


What can be parameterized?
--------------------------

Any data manipulation language (DML) statement can be
parameterized: ``select``, ``insert``, ``update``, and ``delete``.

Schema definition language (SDL) and :ref:`configure
<ref_eql_statements_configure>` statements **cannot** be parameterized. Data
definition language (DDL) has limited support for parameters, but it's not a
recommended pattern. Some of the limitations might be lifted in the future
versions.

