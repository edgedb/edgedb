.. _ref_eql_primer:

Primer
======

EdgeQL is the query language of EdgeDB. It's intended as a spiritual successor
to SQL that solves some of its biggest design limitations. This page is
indended as a rapid-fire overview so you can hit the ground running with
EdgeDB. Refer to the linked pages for more in-depth documentation.

As with most databases, you can write and execute queries as strings with one
of our first-party :ref:`client libraries <ref_clients_index>` for JavaScript,
Python, Golang, and Rust, or you can execute queries :ref:`over HTTP
<ref_edgeql_http>`.

.. note::

  The examples below also demonstrate how to express the query with the
  :ref:`TypeScript query builder <edgedb-js-qb>`, which lets you write
  strongly-typed EdgeQL queries in a code-first way.


Literals
^^^^^^^^

.. tabs::

  .. code-tab:: edgeql-repl

    db> select 'Hello there!';
    {'i ❤️ edgedb'}
    db> select "Hello there!"[0:5];
    {'Hello'}
    db> select false;
    {false}
    db> select 3.14;
    {3.14}
    db> select 12345678n;
    {12345678n}
    db> select 42e+100n;
    {42e100n}
    db> select <uuid>'a5ea6360-75bd-4c20-b69c-8f317b0d2857';
    {a5ea6360-75bd-4c20-b69c-8f317b0d2857}
    db> select <datetime>'1999-03-31T15:17:00Z';
    {<datetime>'1999-03-31T15:17:00Z'}
    db> select <duration>'5 hours 4 minutes 3 seconds';
    {<duration>'5:04:03'}
    db> select <cal::relative_duration>'2 years 18 days';
    {<cal::relative_duration>'P2Y18D'}
    db> select b'bina\\x01ry';
    {b'bina\\x01ry'}

  .. code-tab:: typescript

    e.str("i ❤️ edgedb")
    // string
    e.str("hello there!").slice(0, 5)
    // string
    e.bool(false)
    // boolean
    e.int64(42)
    // number
    e.float64(3.14)
    // number
    e.bigint(BigInt(12345678))
    // bigint
    e.decimal("1234.4567")
    // n/a (not supported by JS clients)
    e.uuid("a5ea6360-75bd-4c20-b69c-8f317b0d2857")
    // string
    e.datetime("1999-03-31T15:17:00Z")
    // Date
    e.duration("5 hours 4 minutes 3 seconds")
    // edgedb.Duration (custom class)
    e.cal.relative_duration("2 years 18 days")
    // edgedb.RelativeDuration (custom class)
    e.bytes(Buffer.from("bina\\x01ry"))
    // Buffer

EdgeDB also supports collection types like arrays, tuples, and a ``json`` type.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select ['hello', 'world'];
    {['hello', 'world']}
    db> select ('Apple', 7, true);
    {('Apple', 7, true)}
    db> select (fruit := 'Apple', quantity := 3.14, fresh := true);
    {(fruit := 'Apple', quantity := 3.14, fresh := true)}
    db> select <json>["this", "is", "an", "array"];
    {"[\"this\", \"is\", \"an\", \"array\"]"}

  .. code-tab:: typescript

    e.array(["hello", "world"]);
    // string[]
    e.tuple(["Apple", 7, true]);
    // [string, number, boolean]
    e.tuple({fruit: "Apple", quantity: 3.14, fresh: true});
    // {fruit: string; quantity: number; fresh: boolean}
    e.json(["this", "is", "an", "array"]);
    // unknown

Refer to :ref:`Docs > EdgeQL > Literals <ref_eql_literals>` for complete docs.

Functions and operators
^^^^^^^^^^^^^^^^^^^^^^^

EdgeDB provides a rich standard library of functions to operate and manipulate
various data types.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select str_upper('oh hi mark');
    {'OH HI MARK'}
    db> select len('oh hi mark');
    {10}
    db> select uuid_generate_v1mc();
    {c68e3836-0d59-11ed-9379-fb98e50038bb}
    db> select contains(['a', 'b', 'c'], 'd');
    {false}

  .. code-tab:: typescript

    e.str_upper("oh hi mark");
    // string
    e.len("oh hi mark");
    // number
    e.uuid_generate_v1mc();
    // string
    e.contains(["a", "b", "c"], "d");
    // boolean

Similarly, it provides a comprehensive set of built-in operators.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select not true;
    {false}
    db> select exists 'hi';
    {true}
    db> select 2 + 2;
    {4}
    db> select 'Hello' ++ ' world!';
    {'Hello world!'}
    db> select '😄' if true else '😢';
    {'😄'}
    db> select <duration>'5 minutes' + <duration>'2 hours';
    {<duration>'2:05:00'}


  .. code-tab:: typescript

    e.op("not", e.bool(true));
    // booolean
    e.op("exists", e.set("hi"));
    // boolean
    e.op("exists", e.cast(e.str, e.set()));
    // boolean
    e.op(e.int64(2), "+", e.int64(2));
    // number
    e.op(e.str("Hello "), "++", e.str("World!"));
    // string
    e.op(e.str("😄"), "if", e.bool(true), "else", e.str("😢"));
    // string
    e.op(e.duration("5 minutes"), "+", e.duration("2 hours"))

See :ref:`Docs > Standard Library <ref_std>` for reference documentation on
all built-in types, functions, and operators.


Insert an object
^^^^^^^^^^^^^^^^

.. tabs::

  .. code-tab:: edgeql-repl

    db> insert Movie {
    ...   title := 'Doctor Strange 2',
    ...   release_year := 2022
    ... };
    {default::Movie {id: 4fb990b6-0d54-11ed-a86c-9b90e88c991b}}


  .. code-tab:: typescript

    const query = e.insert(e.Movie, {
      title: 'Doctor Strange 2',
      release_year: 2022
    });

    const result = await query.run(client);
    // {id: string}
    // by default INSERT only returns
    // the id of the new object

See :ref:`Docs > EdgeQL > Insert <ref_eql_insert>`.

Select objects
^^^^^^^^^^^^^^

Use a *shape* to define which properties to ``select`` from the given object
type.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select Movie {
    ...   id,
    ...   title
    ... };
    {
      default::Movie {
        id: 4fb990b6-0d54-11ed-a86c-9b90e88c991b,
        title: 'Doctor Strange 2'
      },
      ...
    }


  .. code-tab:: typescript

    const query = e.select(e.Movie, () => ({
      id: true,
      title: true
    }));
    const result = await query.run(client);
    // {id: string; title: string; }[]

    // To select all properties of an object, use the
    // spread operator with the special "*"" property:
    const query = e.select(e.Movie, () => ({
      ...e.Movie['*']
    }));

Fetch linked objects with a nested shape.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select Movie {
    ...   id,
    ...   title,
    ...   actors: {
    ...     name
    ...   }
    ... };
    {
      default::Movie {
        id: 9115be74-0979-11ed-8b9a-3bca6792708f,
        title: 'Iron Man',
        actors: {
          default::Person {name: 'Robert Downey Jr.'},
          default::Person {name: 'Gwyneth Paltrow'},
        },
      },
      ...
    }

  .. code-tab:: typescript

    const query = e.select(e.Movie, () => ({
      id: true,
      title: true,
      actors: {
        name: true,
      }
    }));

    const result = await query.run(client);
    // {id: string; title: string, actors: {name: string}[]}[]

See :ref:`Docs > EdgeQL > Select > Shapes <ref_eql_shapes>`.

Filtering, ordering, and pagination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``select`` statement can be augmented with ``filter``, ``order by``,
``offset``, and ``limit`` clauses (in that order).

.. tabs::

  .. code-tab:: edgeql-repl

    db> select Movie {
    ...   id,
    ...   title
    ... }
    ... filter .release_year > 2017
    ... order by .title
    ... offset 10
    ... limit 10;
    {
      default::Movie {
        id: 916425c8-0979-11ed-8b9a-e7c13d25b2ce,
        title: 'Shang Chi and the Legend of the Ten Rings',
      },
      default::Movie {
        id: 91606abe-0979-11ed-8b9a-3f9b41f42697,
        title: 'Spider-Man: Far From Home',
      },
      ...
    }


  .. code-tab:: typescript

    const query = e.select(e.Movie, (movie) => ({
      id: true,
      title: true,
      filter: e.op(movie.release_year, ">", 1999),
      order_by: movie.title,
      offset: 10,
      limit: 10,
    }));

    const result = await query.run(client);
    // {id: string; title: number}[]

See :ref:`Filtering <ref_eql_select_filter>`, :ref:`Ordering
<ref_eql_select_order>`, and :ref:`Pagination <ref_eql_select_pagination>`.

Computed properties
^^^^^^^^^^^^^^^^^^^

Selection shapes can contain computed properties.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select Movie {
    ...   title,
    ...   title_upper := str_upper(.title),
    ...   cast_size := count(.actors)
    ... };
    {
      default::Movie {
        title: 'Guardians of the Galaxy',
        title_upper: 'GUARDIANS OF THE GALAXY',
        cast_size: 8,
      },
      default::Movie {
        title: 'Avengers: Endgame',
        title_upper: 'AVENGERS: ENDGAME',
        cast_size: 30,
      },
      ...
    }

  .. code-tab:: typescript

    e.select(e.Movie, movie => ({
      title: true,
      title_upper: e.str_upper(movie.title),
      cast_size: e.count(movie.actors)
    }))
    // {title: string; title_upper: string; cast_size: number}[]

A common use for computed properties is to query a link in reverse; this is
known as a *backlink* and it has special syntax.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select Person {
    ...   name,
    ...   acted_in := .<actors[is Content] {
    ...     title
    ...   }
    ... };
    {
      default::Person {
        name: 'Dave Bautista',
        acted_in: {
          default::Movie {title: 'Guardians of the Galaxy'},
          default::Movie {title: 'Guardians of the Galaxy Vol. 2'},
          default::Movie {title: 'Avengers: Infinity War'},
          default::Movie {title: 'Avengers: Endgame'},
        },
      },
      ...
    }


  .. code-tab:: typescript

    e.select(e.Person, person => ({
      name: true,
      acted_in: e.select(person["<actors[is Content]"], () => ({
        title: true,
      })),
    }));
    // {name: string; acted_in: {title: string}[];}[]


See :ref:`Docs > EdgeQL > Select > Computed <ref_eql_select>` and
:ref:`Docs > EdgeQL > Select > Backlinks <ref_eql_select>`.

Update objects
^^^^^^^^^^^^^^

The ``update`` statement accepts a ``filter`` clause upfront, followed by a
``set`` shape indicating how the matching objects should be updated.

.. tabs::

  .. code-tab:: edgeql-repl

    db> update Movie
    ... filter .title = "Doctor Strange 2"
    ... set {
    ...   title := "Doctor Strange in the Multiverse of Madness"
    ... };
    {default::Movie {id: 4fb990b6-0d54-11ed-a86c-9b90e88c991b}}


  .. code-tab:: typescript

    const query = e.update(e.Movie, (movie) => ({
      filter: e.op(movie.title, '=', 'Doctor Strange 2'),
      set: {
        title: 'Doctor Strange in the Multiverse of Madness',
      },
    }));

    const result = await query.run(client);
    // {id: string}

When updating links, the set of linked objects can be added to with ``+=``,
subtracted from with ``-=``, or overridden with ``:=``.

.. tabs::

  .. code-tab:: edgeql-repl

    db> update Movie
    ... filter .title = "Doctor Strange 2"
    ... set {
    ...   actors += (select Person filter .name = "Rachel McAdams")
    ... };
    {default::Movie {id: 4fb990b6-0d54-11ed-a86c-9b90e88c991b}}


  .. code-tab:: typescript

    e.update(e.Movie, (movie) => ({
      filter: e.op(movie.title, '=', 'Doctor Strange 2'),
      set: {
        actors: {
          "+=": e.select(e.Person, person => ({
            filter: e.op(person.name, "=", "Rachel McAdams")
          }))
        }
      },
    }));

See :ref:`Docs > EdgeQL > Update <ref_eql_update>`.

Delete objects
^^^^^^^^^^^^^^

The ``delete`` statement can contain ``filter``, ``order by``, ``offset``, and
``limit`` clauses.

.. tabs::

  .. code-tab:: edgeql-repl

    db> delete Movie
    ... filter .ilike "the avengers%"
    ... limit 3;
    {
      default::Movie {id: 3abe2b6e-0d2b-11ed-9ead-3745c7dfd553},
      default::Movie {id: 911cff40-0979-11ed-8b9a-0789a3fd4a02},
      default::Movie {id: 91179c12-0979-11ed-8b9a-3b5c92e7e5a5},
      default::Movie {id: 4fb990b6-0d54-11ed-a86c-9b90e88c991b}
    }


  .. code-tab:: typescript

    const query = e.delete(e.Movie, (movie) => ({
      filter: e.op(movie.title, 'ilike', "the avengers%"),
    }));

    const result = await query.run(client);
    // {id: string}[]

See :ref:`Docs > EdgeQL > Delete <ref_eql_delete>`.


Query parameters
^^^^^^^^^^^^^^^^

.. tabs::

  .. code-tab:: edgeql-repl

    db> insert Movie {
    ...   title := <str>$title,
    ...   release_year := <int64>$release_year
    ... };
    Parameter <str>$title: Thor: Love and Thunder
    Parameter <int64>$release_year: 2022
    {default::Movie {id: 3270a2ec-0d5e-11ed-918b-eb0282058498}}

  .. code-tab:: typescript

    const query = e.params({ title: e.str, release_year: e.int64 }, ($) => {
      return e.insert(e.Movie, {
        title: $.title,
        release_year: $.release_year,
      }))
    };

    const result = await query.run(client, {
      title: 'Thor: Love and Thunder',
      release_year: 2022,
    });
    // {id: string}

Client libraries provide a dedicated API to provide parameters when executing
a query.

.. tabs::

  .. code-tab:: javascript

    import {createClient} from "edgedb";

    const client = createClient();
    const result = await client.query(`select <str>$param`, {
      param: "Play it, Sam."
    });
    // => "Play it, Sam."

  .. code-tab:: python

    import edgedb

    client = edgedb.create_async_client()

    async def main():

        result = await client.query("select <str>$param", param="Play it, Sam")
        # => "Play it, Sam"


  .. code-tab:: golang

    package main

    import (
        "context"
        "log"

        "github.com/edgedb/edgedb-go"
    )

    func main() {
        ctx := context.Background()
        client, err := edgedb.CreateClient(ctx, edgedb.Options{})
        if err != nil {
            log.Fatal(err)
        }
        defer client.Close()

        var (
            param     string = "Play it, Sam."
            result  string
        )

        query := "select <str>$0"
        err = client.Query(ctx, query, &result, param)
        // ...
    }

See :ref:`Docs > EdgeQL > Parameters <ref_eql_params>`.

Subqueries
^^^^^^^^^^

Unlike SQL, EdgeQL is *composable*; queries can be naturally nested. This is
useful, for instance, when performing nested mutations.

.. tabs::

  .. code-tab:: edgeql-repl

    db> with
    ...   dr_strange := (select Movie filter .title = "Doctor Strange"),
    ...   benedicts := (select Person filter .name in {
    ...     'Benedict Cumberbatch',
    ...     'Benedict Wong'
    ...   })
    ... update dr_strange
    ... set {
    ...   actors += benedicts
    ... };
    {default::Movie {id: 913836ac-0979-11ed-8b9a-ef455e591c52}}


  .. code-tab:: typescript

    // select Doctor Strange
    const drStrange = e.select(e.Movie, movie => ({
      filter: e.op(movie.title, '=', "Doctor Strange")
    }));

    // select actors
    const actors = e.select(e.Person, person => ({
      filter: e.op(person.name, 'in', e.set(
        'Benedict Cumberbatch',
        'Benedict Wong'
      ))
    }));

    // add actors to cast of drStrange
    const query = e.update(drStrange, ()=>({
      actors: { "+=": actors }
    }));


We can also use subqueries to fetch properties of an object we just inserted.

.. tabs::

  .. code-tab:: edgeql-repl

    db>  with new_movie := (insert Movie {
    ...    title := "Avengers: The Kang Dynasty",
    ...    release_year := 2025
    ...  })
    ...  select new_movie {
    ...   title, release_year
    ... };
    {
      default::Movie {
        title: 'Avengers: The Kang Dynasty',
        release_year: 2025,
      },
    }


  .. code-tab:: typescript

    // "with" blocks are added automatically
    // in the generated query!

    const newMovie = e.insert(e.Movie, {
      title: "Avengers: The Kang Dynasty",
      release_year: 2025
    });

    const query = e.select(newMovie, ()=>({
      title: true,
      release_year: true,
    }));

    const result = await query.run(client);
    // {title: string; release_year: number;}

See :ref:`Docs > EdgeQL > Select > Subqueries <ref_eql_select_subqueries>`.

Polymorphic queries
^^^^^^^^^^^^^^^^^^^

Consider the following schema.

.. code-block:: sdl

  abstract type Content {
    required property title -> str;
  }

  type Movie extending Content {
    property release_year -> int64;
  }

  type TVShow extending Content {
    property num_seasons -> int64;
  }

We can ``select`` the abstract type ``Content`` to simultaneously fetch all
objects that extend it, and use the ``[is <type>]`` syntax to select
properties from known subtypes.

.. tabs::

  .. code-tab:: edgeql-repl

    db> select Content {
    ...   title,
    ...   [is TVShow].num_seasons,
    ...   [is Movie].release_year
    ... };
    {
      default::TVShow {
        title: 'Wandavision',
        num_seasons: 1,
        release_year: {}
      },
      default::Movie {
        title: 'Iron Man',
        num_seasons: {},
        release_year: 2008
      },
      ...
    }

  .. code-tab:: typescript

    const query = e.select(e.Content, (content) => ({
      title: true,
      ...e.is(e.Movie, {release_year: true}),
      ...e.is(e.TVShow, {num_seasons: true}),
    }));
    /* {
      title: string;
      release_year: number | null;
      num_seasons: number | null;
    }[] */


See :ref:`Docs > EdgeQL > Select > Polymorphic queries
<ref_eql_select_polymorphic>`.


Grouping objects
^^^^^^^^^^^^^^^^

Unlike SQL, EdgeQL provides a top-level ``group`` statement to compute
groupings of objects.

.. tabs::

  .. code-tab:: edgeql-repl

    db> group Movie { title, actors: { name }}
    ... by .release_year;
    {
      {
        key: {release_year: 2008},
        grouping: {'release_year'},
        elements: {
          default::Movie { title: 'Iron Man' },
          default::Movie { title: 'The Incredible Hulk' },
        }
      },
      ...
    }

  .. code-tab:: typescript

    e.group(e.Movie, (movie) => {
      const release_year = movie.release_year;
      return {
        title: true,
        by: {release_year},
      };
    });
    /* {
      grouping: string[];
      key: { release_year: number | null };
      elements: { title: string; }[];
    }[] */


See :ref:`Docs > EdgeQL > Group <ref_eql_group>`.
