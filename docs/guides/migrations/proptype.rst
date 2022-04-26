.. _ref_migration_proptype:

===============================
Changing the type of a property
===============================

This example shows how to change the type of a property. We'll use a
character in an adventure game as the type of data we will evolve.

Let's start with this schema:

.. code-block:: sdl

    type Character {
      required property name -> str;
      required property description -> str;
    }

We edit the schema file and perform our first migration:

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::Character'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00001.edgeql, id:
    m1paw3ogpsdtxaoywd6pl6beg2g64zj4ykhd43zby4eqh64yjad47a
    $ edgedb migrate
    Applied m1paw3ogpsdtxaoywd6pl6beg2g64zj4ykhd43zby4eqh64yjad47a
    (00001.edgeql)

The intent is for the ``description`` to provide some text which
serves both as something to be shown to the player as well as
determining some game actions. Se we end up with something like this:

.. code-block:: edgeql-repl

    db> select Character {name, description};
    {
      default::Character {name: 'Alice', description: 'Tall and strong'},
      default::Character {name: 'Billie', description: 'Smart and aloof'},
      default::Character {name: 'Cameron', description: 'Dashing and smooth'},
    }

However, as we keep developing our game it becomes apparent that this
is less of a "description" and more of a "character class", so at
first we just rename the property to reflect that:

.. code-block:: sdl

    type Character {
      required property name -> str;
      required property class -> str;
    }

The migration gives us this:

.. code-block:: bash

    $ edgedb migration create
    did you rename property 'description' of object type 'default::Character'
    to 'class'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00002.edgeql, id:
    m1ljrgrofsqkvo5hsxc62mnztdhlerxp6ucdto262se6dinhuj4mqq
    $ edgedb migrate
    Applied m1ljrgrofsqkvo5hsxc62mnztdhlerxp6ucdto262se6dinhuj4mqq
    (00002.edgeql)

EdgeDB detected that the change looked like a property was being
renamed, which we confirmed. Since this was an existing property being
renamed, the data is all preserved:

.. code-block:: edgeql-repl

    db> select Character {name, class};
    {
      default::Character {name: 'Alice', class: 'Tall and strong'},
      default::Character {name: 'Billie', class: 'Smart and aloof'},
      default::Character {name: 'Cameron', class: 'Dashing and smooth'},
    }

The contents of the ``class`` property are a bit too verbose, so we
decide to update them. In order for this update to be consistently
applied across several developers, we will make it in the form of a
*data migration*:

.. code-block:: bash

    $ edgedb migration create --allow-empty
    Created ./dbschema/migrations/00003.edgeql, id:
    m1qv2pdksjxxzlnujfed4b6to2ppuodj3xqax4p3r75yfef7kd7jna

Now we can edit the file ``00003.edgeql`` directly:

.. code-block:: edgeql-diff

      CREATE MIGRATION m1qv2pdksjxxzlnujfed4b6to2ppuodj3xqax4p3r75yfef7kd7jna
          ONTO m1ljrgrofsqkvo5hsxc62mnztdhlerxp6ucdto262se6dinhuj4mqq
      {
    +     update default::Character
    +     set {
    +         class :=
    +             'warrior' if .class = 'Tall and strong' else
    +             'scholar' if .class = 'Smart and aloof' else
    +             'rogue'
    +     };
      };

We're ready to apply the migration:

.. code-block:: bash

    $ edgedb migrate
    edgedb error: could not read migrations in ./dbschema/migrations:
    could not read migration file ./dbschema/migrations/00003.edgeql:
    migration name should be
    `m1ryafvp24g5eqjeu65zr4bqf6m3qath3lckfdhoecfncmr7zshehq`
    but `m1qv2pdksjxxzlnujfed4b6to2ppuodj3xqax4p3r75yfef7kd7jna` is used
    instead.
    Migration names are computed from the hash of the migration
    contents. To proceed you must fix the statement to read as:
      CREATE MIGRATION m1ryafvp24g5eqjeu65zr4bqf6m3qath3lckfdhoecfncmr7zshehq
      ONTO ...
    if this migration is not applied to any database. Alternatively,
    revert the changes to the file.

The migration tool detected that we've altered the file and asks us to
update the migration name (acting as a checksum) if this was
deliberate. This is done as a precaution against accidental changes.
Since we've done this on purpose, we can update the file and run
:ref:`ref_cli_edgedb_migrate` again.

As the game becomes more stable there's no reason for the ``class`` to
be a :eql:type:`str` anymore, instead we can use an :eql:type:`enum`
to make sure that we don't accidentally use some invalid value for it.

.. code-block:: sdl

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type Character {
      required property name -> str;
      required property class -> CharacterClass;
    }

Fortunately, we've already updated the ``class`` strings to match the
:eql:type:`enum` values, so that a simple cast will convert all the
values. If we had not done this earlier we would need to do it now in
order for the type change to work.

.. code-block:: bash

    $ edgedb migration create
    did you create scalar type 'default::CharacterClass'? [y,n,l,c,b,s,q,?]
    > y
    did you alter the type of property 'class' of object type
    'default::Character'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00004.edgeql, id:
    m1hc4yynkejef2hh7fvymvg3f26nmynpffksg7yvfksqufif6lulgq
    $ edgedb migrate
    Applied m1hc4yynkejef2hh7fvymvg3f26nmynpffksg7yvfksqufif6lulgq
    (00004.edgeql)

The final migration converted all the ``class`` property values:

.. code-block:: edgeql-repl

    db> select Character {name, class};
    {
      default::Character {name: 'Alice', class: warrior},
      default::Character {name: 'Billie', class: scholar},
      default::Character {name: 'Cameron', class: rogue},
    }
