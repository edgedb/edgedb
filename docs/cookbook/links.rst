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
            multi link cast -> Person;
        }
        type Person {
            required property first_name -> str;
            required property last_name -> str;
        }
    }

In terms of SQL database we defined two foreign keys on the table containing
movies. But link is much more than that.


Nested Sets
-----------

Links allow fetching relatioships using single query:

.. code-block:: edgeql-repl

    tutorial> select Movie {
    .........    director: { first_name },
    .........    cast: { first_name },
    ......... };
    {
        Object {
            director: Object { first_name: 'Denis' },
            cast: {
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

    tutorial> SELECT Movie { title, cast_size:=count(.cast) };
    {Object { title: 'Blade Runner 2049', cast_size: 3 }}

You can find more information on aggregates in the
:ref:`Cookbook <ref_cookbook_aggregates>` and the reference of
:ref:`set functions <ref_eql_operators_set>` (or just search for "aggregate").


.. _ref_cookbook_links_bw:

Backward Links
--------------

In the movie example above, we have only shown a forward link traversal:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie { title, cast: { first_name } };
    {
        Object {
            title: 'Blade Runner 2049',
            cast: {
                Object { first_name: 'Harrison' },
                Object { first_name: 'Ryan' },
                Object { first_name: 'Ana' },
            }
        }
    }

Here is a another example of using forward link. This time we only return
last names of the artists as plain string (not an object). In this case, we
need to alias the field with ``:=``. This example uses **forward link** ``.>``
operator to make the code clearer:

.. code-block:: edgeql-repl

    tutorial> SELECT Movie {
    .........     title,
    .........     starring := Movie.>cast.last_name,
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
``.<`` operator:

.. code-block:: edgeql-repl

    tutorial> SELECT Person {
    .........     first_name,
    .........     movies := Person.<cast[IS Movie].title,
    ......... } FILTER .first_name = 'Ryan';
    {Object {
        first_name: 'Ryan',
        movies: {
            'Blade Runner 2049',
        }
    }}

You might also note that we've added ``[IS Movie]`` cast. This is how backward
links work: they fetch every object in the entire database having the field
``cast`` which is a ``Person``. So we narrow down the set of objects to
``Movie`` and select a title from it.

All other tools work on backward link:

.. code-block:: edgeql-repl

    tutorial> SELECT Person {
    .........     first_name,
    .........     movies := Person.<cast[IS Movie] { title, year }
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
    .........         colleagues := Person.<cast[IS Movie].cast {
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
    .........     collegues := (
    .........         SELECT Person.<cast[IS Movie].cast {
    .........             first_name,
    .........         }
    .........     ),
    ......... } FILTER .first_name = 'Ryan';

Note: we wrapped a backward link access by ``SELECT`` subquery.

Still we can't filter out by ``Person.id != Person.id`` because EdgeDB can't
distinguish them. To make that work we should factor the inner query out:

.. code-block:: edgeql-repl

    tutorial> WITH
    .........     Peer := (SELECT Person.<cast[IS Movie].cast)
    ......... SELECT Person {
    .........     first_name,
    .........     collegues := Peer { first_name },
    ......... } FILTER .first_name = 'Ryan';
    {
        Object {
            first_name: 'Ryan',
            collegues: {
                Object { first_name: 'Ana' },
                Object { first_name: 'Harrison' },
                Object { first_name: 'Ryan' },
            }
        }
    }

Note what, while it looks pretty similar to textual replacement, what we've
actually factored out is "view".  This is the reason of why we have a
``{ first_name }`` in the actual ``colleague`` field not in the ``WITH``
clause.

Now the next step is quite simple, we wrap ``Peer`` selector by a
``SELECT`` subquery and add a filter:

.. code-block:: edgeql-repl

    tutorial> WITH
    .........     Peer := (SELECT Person.<cast[IS Movie].cast { first_name })
    ......... SELECT Person {
    .........     first_name,
    .........     collegues := (SELECT Peer { first_name }
                                FILTER Peer.id != Person.id)
    ......... } FILTER .first_name = 'Ryan';
    .........
    {Object {
        first_name: 'Ryan',
        collegues: {
            Object { first_name: 'Ana' },
            Object { first_name: 'Harrison' },
        }
    }}

.. _ref_cookbook_links_fw_bw:

Forward vs Backward Links
=========================
