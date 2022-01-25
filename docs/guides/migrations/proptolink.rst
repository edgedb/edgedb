.. _ref_migration_proptolink:

=============================
Changing a property to a link
=============================

This example shows how to change a property into a link. We'll use a
character in an adventure game as the type of data we will evolve.

Let's start with this schema:

.. code-block:: sdl

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type Character {
        required property name -> str;
        required property class -> CharacterClass;
    }

We edit the schema file and perform our first migration:

.. code-block:: bash

    $ edgedb migration create
    did you create scalar type 'default::CharacterClass'? [y,n,l,c,b,s,q,?]
    > y
    did you create object type 'default::Character'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00001.edgeql, id:
    m1fg76t7fbvguwhkmzrx7jwki6jxr6dvkswzeepd5v66oxg27ymkcq
    $ edgedb migrate
    Applied m1fg76t7fbvguwhkmzrx7jwki6jxr6dvkswzeepd5v66oxg27ymkcq
    (00001.edgeql)

The initial setup may look something like this:

.. code-block:: edgeql-repl

    db> select Character {name, class};
    {
      default::Character {name: 'Alice', class: warrior},
      default::Character {name: 'Billie', class: scholar},
      default::Character {name: 'Cameron', class: rogue},
    }

After some development work we decide to add more details about the
available classes and encapsulate that information into its own type.
This way instead of a property ``class`` we want to end up with a link
``class`` to the new data structure. Since we cannot just
:eql:op:`cast <cast>` a scalar into an object, we'll need to convert
between the two explicitly. This means that we will need to have both
the old and the new "class" information to begin with:

.. code-block:: sdl

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type NewClass {
        required property name -> str;
        multi property skills -> str;
    }

    type Character {
        required property name -> str;
        required property class -> CharacterClass;
        link new_class -> NewClass;
    }

We update the schema file and migrate to the new state:

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::NewClass'? [y,n,l,c,b,s,q,?]
    > y
    did you create link 'new_class' of object type 'default::Character'?
    [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00002.edgeql, id:
    m1uttd6f7fpiwiwikhdh6qyijb6pcji747ccg2cyt5357i3wsj3l3q
    $ edgedb migrate
    Applied m1uttd6f7fpiwiwikhdh6qyijb6pcji747ccg2cyt5357i3wsj3l3q
    (00002.edgeql)

It makes sense to add a data migration as a way of consistently
creating ``NewClass`` objects as well as populating ``new_class``
links based on the existing ``class`` property. So we first create an
empty migration:

.. code-block:: bash

    $ edgedb migration create --allow-empty
    Created ./dbschema/migrations/00003.edgeql, id:
    m1iztxroh3ifoeqmvxncy77whnaei6tp5j3sewyxtrfysronjkxgga

And then edit the ``00003.edgeql`` file to create and update objects:

.. code-block:: edgeql-diff

      CREATE MIGRATION m1iztxroh3ifoeqmvxncy77whnaei6tp5j3sewyxtrfysronjkxgga
          ONTO m1uttd6f7fpiwiwikhdh6qyijb6pcji747ccg2cyt5357i3wsj3l3q
      {
    +     insert default::NewClass {
    +         name := 'Warrior',
    +         skills := {'punch', 'kick', 'run', 'jump'},
    +     };
    +     insert default::NewClass {
    +         name := 'Scholar',
    +         skills := {'read', 'write', 'analyze', 'refine'},
    +     };
    +     insert default::NewClass {
    +         name := 'Rogue',
    +         skills := {'impress', 'sing', 'steal', 'run', 'jump'},
    +     };
    +
    +    update default::Character
    +    set {
    +        new_class := assert_single((
    +            select default::NewClass
    +            filter .name ilike <str>default::Character.class
    +        )),
    +    };
      };

Trying to apply the data migration will produce the following
reminder:

.. code-block:: bash

    $ edgedb migrate
    edgedb error: could not read migrations in ./dbschema/migrations:
    could not read migration file ./dbschema/migrations/00003.edgeql:
    migration name should be
    `m1e3d3eg3j2pr7acie4n5rrhaddyhkiy5kgckd5l7h5ysrpmgwxl5a` but
    `m1iztxroh3ifoeqmvxncy77whnaei6tp5j3sewyxtrfysronjkxgga` is used
    instead.
    Migration names are computed from the hash of the migration
    contents. To proceed you must fix the statement to read as:
      CREATE MIGRATION m1e3d3eg3j2pr7acie4n5rrhaddyhkiy5kgckd5l7h5ysrpmgwxl5a
      ONTO ...
    if this migration is not applied to any database. Alternatively,
    revert the changes to the file.

The migration tool detected that we've altered the file and asks us to
update the migration name (acting as a checksum) if this was
deliberate. This is done as a precaution against accidental changes.
Since we've done this on purpose, we can update the file and run
:ref:`ref_cli_edgedb_migrate` again.

We can see the changes after the data migration is complete:

.. code-block:: edgeql-repl

    db> select Character {
    ...     name,
    ...     class,
    ...     new_class: {
    ...         name,
    ...     }
    ... };
    {
      default::Character {
        name: 'Alice',
        class: warrior,
        new_class: default::NewClass {name: 'Warrior'},
      },
      default::Character {
        name: 'Billie',
        class: scholar,
        new_class: default::NewClass {name: 'Scholar'},
      },
      default::Character {
        name: 'Cameron',
        class: rogue,
        new_class: default::NewClass {name: 'Rogue'},
      },
    }

Everything seems to be in order. It is time to clean up the old
property and ``CharacterClass`` :eql:type:`enum`:

.. code-block:: sdl

    type NewClass {
        required property name -> str;
        multi property skills -> str;
    }

    type Character {
        required property name -> str;
        link new_class -> NewClass;
    }

The migration tools should have no trouble detecting the things we
just removed:

.. code-block:: bash

    $ edgedb migration create
    did you drop property 'class' of object type 'default::Character'?
    [y,n,l,c,b,s,q,?]
    > y
    did you drop scalar type 'default::CharacterClass'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00004.edgeql, id:
    m1jdnz5bxjj6kjz2pylvudli5rvw4jyr2ilpb4hit3yutwi3bq34ha
    $ edgedb migrate
    Applied m1jdnz5bxjj6kjz2pylvudli5rvw4jyr2ilpb4hit3yutwi3bq34ha
    (00004.edgeql)

Now that the original property and scalar type are gone, we can rename
the "new" components, so that they become ``class`` link and
``CharacterClass`` type, respectively:

.. code-block:: sdl

    type CharacterClass {
        required property name -> str;
        multi property skills -> str;
    }

    type Character {
        required property name -> str;
        link class -> CharacterClass;
    }

The migration tools pick up the changes without any issues again. It
may seem tempting to combine the last two steps, but deleting and
renaming in a single step would cause the migration tools to report a
name clash. As a general rule, it is a good idea to never mix renaming
and deleting of closely interacting entities in the same migration.

.. code-block:: bash

    $ edgedb migration create
    did you rename object type 'default::NewClass' to
    'default::CharacterClass'? [y,n,l,c,b,s,q,?]
    > y
    did you rename link 'new_class' of object type 'default::Character' to
    'class'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00005.edgeql, id:
    m1ra4fhx2erkygbhi7qjxt27yup5aw5hkr5bekn5y5jeam5yn57vsa
    $ edgedb migrate
    Applied m1ra4fhx2erkygbhi7qjxt27yup5aw5hkr5bekn5y5jeam5yn57vsa
    (00005.edgeql)

Finally, we have replaced the original ``class`` property with a link:

.. code-block:: edgeql-repl

    db> select Character {
    ...     name,
    ...     class: {
    ...         name,
    ...         skills,
    ...     }
    ... };
    {
      default::Character {
        name: 'Alice',
        class: default::CharacterClass {
          name: 'Warrior',
          skills: {'punch', 'kick', 'run', 'jump'},
        },
      },
      default::Character {
        name: 'Billie',
        class: default::CharacterClass {
          name: 'Scholar',
          skills: {'read', 'write', 'analyze', 'refine'},
        },
      },
      default::Character {
        name: 'Cameron',
        class: default::CharacterClass {
          name: 'Rogue',
          skills: {'impress', 'sing', 'steal', 'run', 'jump'},
        },
      },
    }
