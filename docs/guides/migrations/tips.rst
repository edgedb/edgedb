.. _ref_migration_tips:

====
Tips
====

:edb-alt-title: Schema migration tips

Adding backlinks
----------------

This example shows how to handle a schema that makes use of a
backlink. We'll use a linked-list structure to represent a sequence of
events.

We'll start with this schema:

.. code-block:: sdl
    :version-lt: 3.0

    type Event {
      required property name -> str;
      link prev -> Event;

      # ... more properties and links
    }

.. code-block:: sdl

    type Event {
      required name: str;
      prev: Event;

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
    :version-lt: 3.0

    type Event {
      required property name -> str;

      link prev -> Event;
      link next := .<prev[is Event];
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Event {
      required name: str;

      prev: Event;
      link next := .<prev[is Event];
    }

.. code-block:: sdl

    type Event {
      required name: str;

      prev: Event;
      next := .<prev[is Event];
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
    :version-lt: 3.0

    type Event {
      required property name -> str;

      link prev -> Event {
        constraint exclusive;
      };
      link next := .<prev[is Event];
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Event {
      required name: str;

      prev: Event {
        constraint exclusive;
      };
      link next := .<prev[is Event];
    }

.. code-block:: sdl

    type Event {
      required name: str;

      prev: Event {
        constraint exclusive;
      };
      next := .<prev[is Event];
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

Making a property required
--------------------------

This example shows how a property may evolve to be more and more
strict over time by looking at a user name field. However, similar
evolution may be applicable to other properties that start off with
few restrictions and gradually become more constrained and formalized
as the needs of the project evolve.

We'll start with a fairly simple schema:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      property name -> str;
    }

.. code-block:: sdl

    type User {
      name: str;
    }

At this stage we don't think that this property needs to be unique or
even required. Perhaps it's only used as a screen name and not as a
way of identifying users.

.. code-block:: bash

    $ edgedb migration create
    did you create object type 'default::User'? [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00001.edgeql, id:
    m14gwyorqqipfg7riexvbdq5dhgv7x6buqw2jaaulilcmywinmakzq
    $ edgedb migrate
    Applied m14gwyorqqipfg7riexvbdq5dhgv7x6buqw2jaaulilcmywinmakzq
    (00001.edgeql)

We've got our first migration to set up the schema. Now after using
that for a little while we realize that we want to make ``name`` a
*required property*. So we make the following change in the schema
file:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str;
    }

.. code-block:: sdl

    type User {
      required name: str;
    }

Next we try to migrate:

.. code-block:: bash

    $ edgedb migration create
    did you make property 'name' of object type 'default::User' required?
    [y,n,l,c,b,s,q,?]
    > y
    Please specify an expression to populate existing objects in order to make
    property 'name' of object type 'default::User' required:
    fill_expr> 'change me'

Oh! That's right, we can't just make ``name`` *required* because there
could be existing ``User`` objects without a ``name`` at all. So we
need to provide some kind of placeholder value for those cases. We
type ``'change me'`` (although any other string would do, too). This is
different from specifying a ``default`` value since it will be applied
to *existing* objects, whereas the ``default`` applies to *new ones*.

Unseen to us (unless we take a look at the automatically generated
``.edgeql`` files inside our ``/dbschema`` folder), EdgeDB has created
a migration script that includes the following command to make our
schema change happen.

.. code-block:: edgeql

  ALTER TYPE default::User {
      ALTER PROPERTY name {
          SET REQUIRED USING (<std::str>'change me');
      };
  };

We then run :ref:`ref_cli_edgedb_migrate` to apply the changes.

Next we realize that we actually want to make names unique, perhaps to
avoid confusion or to use them as reliable human-readable identifiers
(unlike ``id``). We update the schema again:

.. code-block:: sdl
    :version-lt: 3.0

    type User {
      required property name -> str {
        constraint exclusive;
      }
    }

.. code-block:: sdl

    type User {
      required name: str {
        constraint exclusive;
      }
    }

Now we proceed with the migration:

.. code-block:: bash

    $ edgedb migration create
    did you create constraint 'std::exclusive' of property 'name'?
    [y,n,l,c,b,s,q,?]
    > y
    Created ./dbschema/migrations/00003.edgeql, id:
    m1dxs3xbk4f3vhmqh6mjzetojafddtwlphp5a3kfbfuyvupjafevya
    $ edgedb migrate
    edgedb error: ConstraintViolationError: name violates exclusivity
    constraint

Some objects must have the same ``name``, so the migration can't be
applied. We have a couple of options for fixing this:

1) Review the existing data and manually :eql:stmt:`update` the
   entries with duplicate names so that they are unique.
2) Edit the migration to add an :eql:stmt:`update` which will
   de-duplicate ``name`` for any potential existing ``User`` objects.

The first option is good for situations where we want to signal to any
other maintainer of a copy of this project that they need to make a
decision about handling name duplicates in whatever way is appropriate
to them without making an implicit decision once and for all.

Here we will go with the second option, which is good for situations
where we know enough about the situation that we can make a decision
now and never have to duplicate this effort for any other potential
copies of our project.

We edit the last migration file ``00003.edgeql``:

.. code-block:: edgeql-diff

      CREATE MIGRATION m1dxs3xbk4f3vhmqh6mjzetojafddtwlphp5a3kfbfuyvupjafevya
          ONTO m1ndhbxx7yudb2dv7zpypl2su2oygyjlggk3olryb5uszofrfml4uq
      {
    +   with U := default::User
    +   update default::User
    +   filter U.name = .name and U != default::User
    +   set {
    +     # De-duplicate names by appending a random uuid.
    +     name := .name ++ '_' ++ <str>uuid_generate_v1mc()
    +   };
    +
        ALTER TYPE default::User {
            ALTER PROPERTY name {
                CREATE CONSTRAINT std::exclusive;
            };
        };
      };

And then we apply the migration:

.. code-block:: bash

    $ edgedb migrate
    edgedb error: could not read migrations in ./dbschema/migrations: could not
    read migration file ./dbschema/migrations/00003.edgeql: migration name
    should be `m1t6slgcfne35vir2lcgnqkmaxsxylzvn2hanr6mijbj5esefsp7za` but `
    m1dxs3xbk4f3vhmqh6mjzetojafddtwlphp5a3kfbfuyvupjafevya` is used instead.
    Migration names are computed from the hash of the migration contents. To
    proceed you must fix the statement to read as:
      CREATE MIGRATION m1t6slgcfne35vir2lcgnqkmaxsxylzvn2hanr6mijbj5esefsp7za
      ONTO ...
    if this migration is not applied to any database. Alternatively, revert the
    changes to the file.

The migration tool detected that we've altered the file and asks us to
update the migration name (acting as a checksum) if this was
deliberate. This is done as a precaution against accidental changes.
Since we've done this on purpose, we can update the file and run
:ref:`ref_cli_edgedb_migrate` again.

Finally, we evolved our schema all the way from having an optional
property ``name`` all the way to making it both *required* and
*exclusive*. We've worked with the EdgeDB :ref:`migration tools
<ref_cli_edgedb_migration>` to iron out the kinks throughout the
migration process. At this point we take a quick look at the way
duplicate ``User`` objects were resolved to decide whether we need to
do anything more. We can use :eql:func:`re_test` to find names that
look like they are ending in a UUID:

.. code-block:: edgeql-repl

    db> select User { name }
    ... filter
    ...     re_test('.* [a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}$', .name);
    {
      default::User {name: 'change me bc30d45a-2bcf-11ec-a6c2-6ff21f33a302'},
      default::User {name: 'change me bc30d8a6-2bcf-11ec-a6c2-4f739d559598'},
    }

Looks like the only duplicates are the users that had no names
originally and that never updated the ``'change me'`` placeholders, so
we can probably let them be for now. In hindsight, it may have been a
good idea to use UUID-based names to populate the empty properties
from the very beginning.

Changing a property to a link
-----------------------------

This example shows how to change a property into a link. We'll use a
character in an adventure game as the type of data we will evolve.

Let's start with this schema:

.. code-block:: sdl
    :version-lt: 3.0

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type Character {
      required property name -> str;
      required property class -> CharacterClass;
    }

.. code-block:: sdl

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type Character {
      required name: str;
      required class: CharacterClass;
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
    :version-lt: 3.0

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

.. code-block:: sdl

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type NewClass {
      required name: str;
      multi skills: str;
    }

    type Character {
      required name: str;
      required class: CharacterClass;
      new_class: NewClass;
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
    +    insert default::NewClass {
    +        name := 'Warrior',
    +        skills := {'punch', 'kick', 'run', 'jump'},
    +    };
    +    insert default::NewClass {
    +        name := 'Scholar',
    +        skills := {'read', 'write', 'analyze', 'refine'},
    +    };
    +    insert default::NewClass {
    +        name := 'Rogue',
    +        skills := {'impress', 'sing', 'steal', 'run', 'jump'},
    +    };
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
    :version-lt: 3.0

    type NewClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      link new_class -> NewClass;
    }

.. code-block:: sdl

    type NewClass {
      required name: str;
      multi skills: str;
    }

    type Character {
      required name: str;
      new_class: NewClass;
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
    :version-lt: 3.0

    type CharacterClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      link class -> CharacterClass;
    }

.. code-block:: sdl

    type CharacterClass {
      required name: str;
      multi skills: str;
    }

    type Character {
      required name: str;
      class: CharacterClass;
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

Changing the type of a property
-------------------------------

This example shows how to change the type of a property. We'll use a
character in an adventure game as the type of data we will evolve.

Let's start with this schema:

.. code-block:: sdl
    :version-lt: 3.0

    type Character {
      required property name -> str;
      required property description -> str;
    }

.. code-block:: sdl

    type Character {
      required name: str;
      required description: str;
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
    :version-lt: 3.0

    type Character {
      required property name -> str;
      required property class -> str;
    }

.. code-block:: sdl

    type Character {
      required name: str;
      required class: str;
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
    :version-lt: 3.0

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type Character {
      required property name -> str;
      required property class -> CharacterClass;
    }

.. code-block:: sdl

    scalar type CharacterClass extending enum<warrior, scholar, rogue>;

    type Character {
      required name: str;
      required class: CharacterClass;
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

Adding a required link
----------------------

This example shows how to setup a required link. We'll use a
character in an adventure game as the type of data we will evolve.

Let's start with this schema:

.. code-block:: sdl
    :version-lt: 3.0

    type Character {
      required property name -> str;
    }

.. code-block:: sdl

    type Character {
      required name: str;
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
schema and data. Unlike in the scenario when changing a property
to a link, we will add the ``required link class`` right away,
without any intermediate properties. So we end up with a schema
like this:

.. code-block:: sdl
    :version-lt: 3.0

    type CharacterClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      required link class -> CharacterClass;
    }

.. code-block:: sdl

    type CharacterClass {
      required name: str;
      multi skills: str;
    }

    type Character {
      required name: str;
      required class: CharacterClass;
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

Uh-oh! Unlike in a situation with a required property, it's not a good
idea to just :eql:stmt:`insert` a new ``CharacterClass`` object for
every character. So we should abort this migration attempt and rethink
our strategy. We need a separate step where the ``class`` link is
not *required* so that we can write some custom queries to handle
the character classes:

.. code-block:: sdl
    :version-lt: 3.0

    type CharacterClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      link class -> CharacterClass;
    }

.. code-block:: sdl

    type CharacterClass {
      required name: str;
      multi skills: str;
    }

    type Character {
      required name: str;
      class: CharacterClass;
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
    :version-lt: 3.0

    type CharacterClass {
      required property name -> str;
      multi property skills -> str;
    }

    type Character {
      required property name -> str;
      required link class -> CharacterClass;
    }

.. code-block:: sdl

    type CharacterClass {
      required name: str;
      multi skills: str;
    }

    type Character {
      required name: str;
      required class: CharacterClass;
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

Recovering lost migrations
--------------------------

You can recover lost migration files, writing the database's current
migration history to ``/dbschema/migrations`` by using the
:ref:`ref_cli_edgedb_migration_extract`.

Getting the current migration
-----------------------------

The following query will return the most current migration:

.. code-block:: edgeql-repl

    db> with
    ...  module schema,
    ...  lastMigration := (
    ...    select Migration filter not exists .<parents[is Migration]
    ...  )
    ... select lastMigration {*};
