.. _ref_cookbook_links:

=====
Links
=====

Link items define a specific relationship between two object types.
Links have a direction, but can be traversed in both ways :ref:`forward and
backward <ref_cookbook_links_fw_bw>`.


Movie Example
=============

To define a relationship we use ``link`` keyword:

.. code-block:: edgeql

    module default {
        type Movie {
            required property title -> str;
            required link director -> Person;
            multi link actors -> Person;
        }
        type Person {
            required property first_name -> str;
            required property last_name -> str;
        }
    }




Nested Sets
-----------

Links allow fetching relationships with a single query:

.. code-block:: edgeql-repl

    tutorial> select Movie {
    .........    director: { first_name },
    .........    actors: { first_name },
    ......... };
    {
        Object {
            director: Object { first_name: 'Denis' },
            actors: {
                Object { first_name: 'Harrison' },
                Object { first_name: 'Ryan' },
                Object { first_name: 'Ana' },
            }
        }
    }

Aggregates
----------

Similarly you can run some aggregates on the nested sets:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie { title, actors_number:=count(.actors) };
    {Object { title: 'Blade Runner 2049', actors_number: 3 }}

You can find more information on aggregates in the
:ref:`Cookbook <ref_cookbook_aggregates>` and the reference of
:ref:`set functions <ref_eql_operators_set>` (or just search for "aggregate").


.. _ref_cookbook_links_bw:

Backward Links
--------------

In the movie example above, we have only shown a forward link traversal:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie { title, actors: { first_name } };
    {
        Object {
            title: 'Blade Runner 2049',
            actors: {
                Object { first_name: 'Harrison' },
                Object { first_name: 'Ryan' },
                Object { first_name: 'Ana' },
            }
        }
    }

Here is a another example of using forward link. This time we only return
last names of the artists as plain string (not an object). In this case, we
need to alias the field with ``:=``:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie {
    .........     title,
    .........     starring := Movie.actors.last_name,
    ......... };
    {Object {
        title: 'Blade Runner 2049',
        starring: {
            'Ford',
            'Gosling',
            'de Armas',
        }
    }}

To find all movies that a person is starred in we use a **backward link**
traversal ``.<`` operator:

.. code-block:: edgeql-repl

    tutorial> SELECT Person {
    .........     first_name,
    .........     movies := Person.<actors[IS Movie].title,
    ......... } FILTER .first_name = 'Ryan';
    {Object {
        first_name: 'Ryan',
        movies: {
            'Blade Runner 2049',
        }
    }}

You might also note that we've added ``[IS Movie]`` type filter. This is how
backward link traversal works: EdgeDB fetches every object in the entire
database having the field ``actors`` which is a ``Person``. So we narrow down
the set of objects to ``Movie`` and select a title from it.

All other tools work on backward link:

.. code-block:: edgeql-repl

    tutorial> SELECT Person {
    .........     first_name,
    .........     movies := Person.<actors[IS Movie] { title, year }
    ......... } FILTER .first_name = 'Ryan';
    {Object {
        first_name: 'Ryan',
        movies: {
            Object { title: 'Blade Runner 2049', year: 2017 },
        }
    }}

Or more complex example:

.. code-block:: edgeql-repl

    tutorial>     SELECT Person {
    .........         first_name,
    .........         colleagues := Person.<actors[IS Movie].actors {
    .........             first_name
    .........         }
    .........     } FILTER .first_name = 'Ryan';
    {
        Object {
            first_name: 'Ryan',
            colleagues: {
                Object { first_name: 'Ana' },
                Object { first_name: 'Harrison' },
                Object { first_name: 'Ryan' },
            }
        }
    }

Now you may notice that the Ryan (Gosling) is mentioned as a colleague of
himself. To fix it we need few more concepts.

First note that the request above is an equivalent of:

.. code-block:: edgeql-repl

    tutorial> SELECT Person {
    .........     first_name,
    .........     colleagues := (
    .........         SELECT Person.<actors[IS Movie].actors {
    .........             first_name,
    .........         }
    .........     ),
    ......... } FILTER .first_name = 'Ryan';

Note: we wrapped a backward link access by ``SELECT`` subquery.

Still we can't filter out by ``Person != Person`` because EdgeDB can't
distinguish them. To make that work we should give the inner query an alias:

.. code-block:: edgeql-repl

    tutorial> SELECT Person {
    .........     first_name,
    .........     colleagues := (
    .........         WITH Peer := Person.<actors[IS Movie].actors
    .........         SELECT Peer {
    .........             first_name,
    .........         }
    .........     ),
    ......... } FILTER .first_name = 'Ryan';

The ``WITH`` clause makes an alias to the :ref:`set <ref_eql_fundamentals_set>`
of actors. And then we can work with that set as usual.

Now the next step is quite simple, just add a filter:

.. code-block:: edgeql-repl

    tutorial> SELECT Person {
    .........     first_name,
    .........     colleagues := (
    .........         WITH Peer := Person.<actors[IS Movie].actors
    .........         SELECT Peer { first_name }
    .........         FILTER Peer != Person
    .........     ),
    ......... } FILTER .first_name = 'Ryan';
    {Object {
        first_name: 'Ryan',
        collegues: {
            Object { first_name: 'Ana' },
            Object { first_name: 'Harrison' },
        }
    }}

Note also how elegantly use ``Peer != Person`` instead of
``Peer.id != Person.id`` to compare object identity.
