.. _ref_migration_names:

==========================
Making a property required
==========================

This example shows how a property may evolve to be more and more
strict over time by looking at a user name field. However, similar
evolution may be applicable to other properties that start off with
few restrictions and gradually become more constrained and formalized
as the needs of the project evolve.

We'll start with a fairly simple schema:

.. code-block:: sdl

    type User {
        property name -> str;
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

    type User {
        required property name -> str;
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
We then run :ref:`ref_cli_edgedb_migrate` to apply the changes.

Next we realize that we actually want to make names unique, perhaps to
avoid confusion or to use them as reliable human-readable identifiers
(unlike ``id``). We update the schema again:

.. code-block:: sdl

    type User {
        required property name -> str {
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
