.. _ref_migration_backlink:

================
Adding backlinks
================

This example shows how to handle a schema that makes use of a
backlink. We'll use a linked-list structure to represent a sequence of
events.

We'll start with this schema:

.. code-block:: sdl

    type Event {
      required property name -> str;
      link prev -> Event;

      # ... more properties and links
    }

We specify a ``prev`` link because that will make adding a new
``Event`` at the end of the chain easier, since we'll be able to
specify the payload and the chain the ``Event`` should be appended to
in a single :eql:stmt:`insert`. Once we've updated the schema
file we proceed with our first migration:

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::Event'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00001.edgeql, id:
    m1v3ahcx5f43y6mlsdmlz2agnf6msbc7rt3zstiqmezaqx4ev2qovq
    $ edgedb migrate
    Applied m1v3ahcx5f43y6mlsdmlz2agnf6msbc7rt3zstiqmezaqx4ev2qovq
    (00001.edgeql)

We now have a way of chaining events together. We might create a few
events like these:

.. code-block:: edgeql-repl

    db> select Event {
    ...     name,
    ...     prev: { name },
    ... };
    {
      default::Event {name: 'setup', prev: {}},
      default::Event {name: 'work', prev: default::Event {name: 'setup'}},
      default::Event {name: 'cleanup', prev: default::Event {name: 'work'}},
    }

It seems like having a ``next`` link would be useful, too. So we can
define it as a computed link by using :ref:`backlink
<ref_datamodel_links>` notation:

.. code-block:: sdl

    type Event {
      required property name -> str;

      link prev -> Event;
      link next := .<prev[is Event];
    }

The migration is straightforward enough:

.. code-block:: bash

    $ edgedb migration create
    did you create link 'next' of object type 'default::Event'?
    [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00002.edgeql, id:
    m1qpukyvw2m4lmomoseni7vdmevk4wzgsbviojacyrqgiyqjp5sdsa
    $ edgedb migrate
    Applied m1qpukyvw2m4lmomoseni7vdmevk4wzgsbviojacyrqgiyqjp5sdsa
    (00002.edgeql)

Trying out the new link on our existing data gives us:

.. code-block:: edgeql-repl

    db> select Event {
    ...     name,
    ...     prev_name := .prev.name,
    ...     next_name := .next.name,
    ... };
    {
      default::Event {
        name: 'setup',
        prev_name: {},
        next_name: {'work'},
      },
      default::Event {
        name: 'work',
        prev_name: 'setup',
        next_name: {'cleanup'},
      },
      default::Event {
        name: 'cleanup',
        prev_name: 'work',
        next_name: {},
      },
    }

That's not quite right. The value of ``next_name`` appears to be a set
rather than a singleton. This is because the link ``prev`` is
many-to-one and so ``next`` is one-to-many, making it a *multi* link.
Let's fix that by making the link ``prev`` a one-to-one, after all
we're interested in building event chains, not trees.

.. code-block:: sdl

    type Event {
      required property name -> str;

      link prev -> Event {
        constraint exclusive;
      };
      link next := .<prev[is Event];
    }

Since the ``next`` link is computed, the migration should not need any
additional user input even though we're reducing the link's
cardinality:

.. code-block:: bash

    $ edgedb migration create
    did you create constraint 'std::exclusive' of link 'prev'?
    [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00003.edgeql, id:
    m17or2bfywuckdqeornjmjh7c2voxgatspcewyefcd4p2vbdepimoa
    $ edgedb migrate
    Applied m17or2bfywuckdqeornjmjh7c2voxgatspcewyefcd4p2vbdepimoa
    (00003.edgeql)

The new ``next`` computed link is now inferred as a ``single`` link
and so the query results for ``next_name`` and ``prev_name`` are
symmetrical:

.. code-block:: edgeql-repl

    db> select Event {
    ...     name,
    ...     prev_name := .prev.name,
    ...     next_name := .next.name,
    ... };
    {
      default::Event {name: 'setup', prev_name: {}, next_name: 'work'},
      default::Event {name: 'work', prev_name: 'setup', next_name: 'cleanup'},
      default::Event {name: 'cleanup', prev_name: 'work', next_name: {}},
    }
