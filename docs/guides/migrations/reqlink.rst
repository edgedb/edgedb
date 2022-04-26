.. _ref_migration_reqlink:

======================
Adding a required link
======================

This example shows how to setup a required link. We'll use a
character in an adventure game as the type of data we will evolve.

Let's start with this schema:

.. code-block:: sdl

    type Character {
      required property name -> str;
    }

We edit the schema file and perform our first migration:

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::Character'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00001.edgeql, id:
    m1xvu7o4z5f5xfwuun2vee2cryvvzh5lfilwgkulmqpifo5m3dnd6a
    $ edgedb migrate
    Applied m1xvu7o4z5f5xfwuun2vee2cryvvzh5lfilwgkulmqpifo5m3dnd6a
    (00001.edgeql)

This time around let's practice performing a data migration and set up
our character data. For this purpose we can create an empty migration
and fill it out as we like:

.. code-block:: bash

    $ edgedb migration create --allow-empty
    Created ./dbschema/migrations/00002.edgeql, id:
    m1lclvwdpwitjj4xqm45wp74y4wjyadljct5o6bsctlnh5xbto74iq

We edit the ``00002.edgeql`` file by simply adding the query to add
characters to it. We can use :eql:stmt:`for` to add multiple characters
like this:

.. code-block:: edgeql-diff

      CREATE MIGRATION m1lclvwdpwitjj4xqm45wp74y4wjyadljct5o6bsctlnh5xbto74iq
          ONTO m1xvu7o4z5f5xfwuun2vee2cryvvzh5lfilwgkulmqpifo5m3dnd6a
      {
    +     for name in {'Alice', 'Billie', 'Cameron', 'Dana'}
    +     union (
    +         insert default::Character {
    +             name := name
    +         }
    +     );
      };

Trying to apply the data migration will produce the following
reminder:

.. code-block:: bash

    $ edgedb migrate
    edgedb error: could not read migrations in ./dbschema/migrations:
    could not read migration file ./dbschema/migrations/00002.edgeql:
    migration name should be
    `m1juin65wriqmb4vwg23fiyajjxlzj2jyjv5qp36uxenit5y63g2iq` but
    `m1lclvwdpwitjj4xqm45wp74y4wjyadljct5o6bsctlnh5xbto74iq` is used instead.
    Migration names are computed from the hash of the migration contents. To
    proceed you must fix the statement to read as:
      CREATE MIGRATION m1juin65wriqmb4vwg23fiyajjxlzj2jyjv5qp36uxenit5y63g2iq
      ONTO ...
    if this migration is not applied to any database. Alternatively,
    revert the changes to the file.

The migration tool detected that we've altered the file and asks us to
update the migration name (acting as a checksum) if this was
deliberate. This is done as a precaution against accidental changes.
Since we've done this on purpose, we can update the file and run
:ref:`ref_cli_edgedb_migrate` again.

.. code-block:: edgeql-diff

    - CREATE MIGRATION m1lclvwdpwitjj4xqm45wp74y4wjyadljct5o6bsctlnh5xbto74iq
    + CREATE MIGRATION m1juin65wriqmb4vwg23fiyajjxlzj2jyjv5qp36uxenit5y63g2iq
          ONTO m1xvu7o4z5f5xfwuun2vee2cryvvzh5lfilwgkulmqpifo5m3dnd6a
      {
          # ...
      };

After we apply the data migration we should be able to see the added
characters:

.. code-block:: edgeql-repl

    db> select Character {name};
    {
      default::Character {name: 'Alice'},
      default::Character {name: 'Billie'},
      default::Character {name: 'Cameron'},
      default::Character {name: 'Dana'},
    }

Let's add a character ``class`` represented by a new type to our
schema and data. Unlike in :ref:`this scenario <ref_migration_proptolink>`,
we will add the ``required link class`` right away, without any intermediate
properties. So we end up with a schema like this:

.. code-block:: sdl

    type CharacterClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      required link class -> CharacterClass;
    }

We go ahead and try to apply this new schema:

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::CharacterClass'? [y,n,l,c,b,s,q,?]
    > y
    did you create link 'class' of object type 'default::Character'?
    [y,n,l,c,b,s,q,?]
    > y
    Please specify an expression to populate existing objects in order to make
    link 'class' of object type 'default::Character' required:
    fill_expr>

Uh-oh! Unlike in a situation with a :ref:`required property
<ref_migration_names>`, it's not a good idea to just
:eql:stmt:`insert` a new ``CharacterClass`` object for every
character. So we should abort this migration attempt and rethink
our strategy. We need a separate step where the ``class`` link is
not *required* so that we can write some custom queries to handle
the character classes:

.. code-block:: sdl

    type CharacterClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      link class -> CharacterClass;
    }

We can now create a migration for our new schema, but we won't apply
it right away:

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::CharacterClass'? [y,n,l,c,b,s,q,?]
    > y
    did you create link 'class' of object type 'default::Character'?
    [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00003.edgeql, id:
    m1jie3xamsm2b7ygqccwfh2degdi45oc7mwuyzjkanh2qwgiqvi2ya

We don't need to create a blank migration to add data, we can add our
modifications into the migration that adds the ``class`` link
directly. Doing this makes sense when the schema changes seem to
require the data migration and the two types of changes logically go
together. We will need to create some ``CharacterClass`` objects as
well as :eql:stmt:`update` the ``class`` link on existing
``Character`` objects:

.. code-block:: edgeql-diff

      CREATE MIGRATION m1jie3xamsm2b7ygqccwfh2degdi45oc7mwuyzjkanh2qwgiqvi2ya
          ONTO m1juin65wriqmb4vwg23fiyajjxlzj2jyjv5qp36uxenit5y63g2iq
      {
        CREATE TYPE default::CharacterClass {
            CREATE REQUIRED PROPERTY name -> std::str;
            CREATE MULTI PROPERTY skills -> std::str;
        };
        ALTER TYPE default::Character {
            CREATE LINK class -> default::CharacterClass;
        };

    +   insert default::CharacterClass {
    +       name := 'Warrior',
    +       skills := {'punch', 'kick', 'run', 'jump'},
    +   };
    +   insert default::CharacterClass {
    +       name := 'Scholar',
    +       skills := {'read', 'write', 'analyze', 'refine'},
    +   };
    +   insert default::CharacterClass {
    +       name := 'Rogue',
    +       skills := {'impress', 'sing', 'steal', 'run', 'jump'},
    +   };
    +   # All warriors
    +   update default::Character
    +   filter .name in {'Alice'}
    +   set {
    +       class := assert_single((
    +           select default::CharacterClass
    +           filter .name = 'Warrior'
    +       )),
    +   };
    +   # All scholars
    +   update default::Character
    +   filter .name in {'Billie'}
    +   set {
    +       class := assert_single((
    +           select default::CharacterClass
    +           filter .name = 'Scholar'
    +       )),
    +   };
    +   # All rogues
    +   update default::Character
    +   filter .name in {'Cameron', 'Dana'}
    +   set {
    +       class := assert_single((
    +           select default::CharacterClass
    +           filter .name = 'Rogue'
    +       )),
    +   };
      };

In a real game we might have a lot more characters and so a good way
to update them all is to update characters of the same class in bulk.

Just like before we'll be reminded to fix the migration name since
we've altered the migration file. After fixing the migration hash we
can apply it. Now all our characters should have been assigned their
classes:

.. code-block:: edgeql-repl

    db> select Character {
    ...     name,
    ...     class: {
    ...         name
    ...     }
    ... };
    {
      default::Character {
        name: 'Alice',
        class: default::CharacterClass {name: 'Warrior'},
      },
      default::Character {
        name: 'Billie',
        class: default::CharacterClass {name: 'Scholar'},
      },
      default::Character {
        name: 'Cameron',
        class: default::CharacterClass {name: 'Rogue'},
      },
      default::Character {
        name: 'Dana',
        class: default::CharacterClass {name: 'Rogue'},
      },
    }

We're finally ready to make the ``class`` link *required*. We update
the schema:

.. code-block:: sdl

    type CharacterClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      required link class -> CharacterClass;
    }

And we perform our final migration:

.. code-block:: bash

    $ edgedb migration create
    did you make link 'class' of object type 'default::Character' required?
    [y,n,l,c,b,s,q,?]
    > y
    Please specify an expression to populate existing objects in order to
    make link 'class' of object type 'default::Character' required:
    fill_expr> assert_exists(.class)
    Created ./dbschema/migrations/00004.edgeql, id:
    m14yblybdo77c7bjtm6nugiy5cs6pl6rnuzo5b27gamy4zhuwjifia

The migration system doesn't know that we've already assigned ``class`` values
to all the ``Character`` objects, so it still asks us for an expression to be
used in case any of the objects need it. We can use ``assert_exists(.class)``
here as a way of being explicit about the fact that we expect the values to
already be present. Missing values would have caused an error even without the
``assert_exists`` wrapper, but being explicit may help us capture the intent
and make debugging a little easier if anyone runs into a problem at this step.

In fact, before applying this migration, let's actually add a new
``Character`` to see what happens:

.. code-block:: edgeql-repl

    db> insert Character {name := 'Eric'};
    {
      default::Character {
        id: 9f4ac7a8-ac38-11ec-b076-afefd12d7e66,
      },
    }

Our attempt at migrating fails as we expected:

.. code-block:: bash

    $ edgedb migrate
    edgedb error: MissingRequiredError: missing value for required link 
    'class' of object type 'default::Character'
      Detail: Failing object id is 'ee604992-c1b1-11ec-ad59-4f878963769f'.

After removing the bugged ``Character``, we can migrate without any problems:

.. code-block:: bash

    $ edgedb migrate
    Applied m14yblybdo77c7bjtm6nugiy5cs6pl6rnuzo5b27gamy4zhuwjifia
    (00004.edgeql)
