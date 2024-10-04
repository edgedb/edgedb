.. _ref_migration_guide:

======
Basics
======

:edb-alt-title: Schema migration basics

EdgeQL is a strongly-typed language, which means that it moves checks
and verification of your code to compile time as much as possible
instead of performing them at run time. EdgeDB's view is that a schema
should allow you to set types, constraints, expressions, and more so that
you can confidently know what sort of behavior to expect from your data.
Laying a type-safe foundation means a bit more thinking up front, but saves
you all kinds of headaches down the road.

It's unlikely though that you'll define your schema perfectly on your first
try, or that you'll build an application that never needs its schema revised.
When you *do* eventually need to make a change, you'll need to migrate
your schema from its current state to a new state.

The basics of creating a project, modifying its schema, and migrating
it in EdgeDB are pretty easy:

- Type ``edgedb project init`` to start a project,
- Open the newly created empty schema at ``dbschema/default.esdl`` and add
  a simple type like  ``SomeType { name: str; }`` inside the empty ``module``
- Run ``edgedb migration create``, type ``y`` to confirm the change,
  then run ``edgedb migrate``, and you are done! You can now
  ``insert SomeType;`` in your database to your heart's content.

.. note::

   If you ever feel like outright removing and creating an instance anew
   during this migration guide, you can use the command
   ``edgedb instance destroy -I <instancename> --force``. And if you want to
   remove all existing migrations as well, you can manually delete them inside
   your ``/migrations`` folder (otherwise, the CLI will try to apply the
   migrations again when you recreate your instance with
   ``edgedb migration create``). Once that is done, you will have a blank
   slate on which to start over again.

But many EdgeDB users have needs that go beyond these basics. In addition,
schema migrations are pretty interesting and teach you a lot about
what EdgeDB does behind the scenes. This guide will turn you from
a casual migration user into one with a lot more tools at hand, along
with a deeper understanding of the internals of EdgeDB at the same
time.

EdgeDB's built-in tools are what make schema migrations easy, and
the way they work is through a pretty interesting interaction between
EdgeDB's SDL (Schema Definition Language) and DDL (Data Definition
Language). The first thing to understand about migrations is the difference
between SDL and DDL, and how they are used.

SDL: For humans
===============

SDL, not DDL, is the primary way for you to create and migrate your
schema in EdgeDB. You don't need to work with DDL to use EdgeDB any
more than you need to know how to change a tire to drive a car.

SDL is built for humans to read, which is why it is said to be *declarative*.
The 'clar' inside *declarative* is the same word as *clear*, and this
is exactly what declarative means: making it *clear* what you want
the final result to be. An example of a declarative instruction in
real life would be telling a friend to show up at your house at 6416
Riverside Way. You've declared what the final result should be, but
it's up to your friend to find how to achieve it.

Now let's look at some real SDL and think about its role in EdgeDB.
Here is a simple example of a schema:

.. code-block:: sdl

   module default {
     type User {
       name: str;
     }
   }

If you have EdgeDB installed and want to follow along, type ``edgedb
project init`` and copy the above schema into your ``default.esdl``
file inside the ``/dbschema`` folder it creates. Then save the file.

.. note::

    While schema is usually contained inside the ``default.esdl`` file,
    you can divide a schema over multiple files if you like. EdgeDB will
    combine all ``.esdl`` files inside the ``/dbschema`` folder into a
    single schema.

Type ``edgedb`` to start the EdgeDB REPL, and, into the REPL,  type
``describe schema as sdl``. The output will be ``{'module default{};'}``
— nothing more than the empty ``default`` module. What happened?
Our ``type User`` is nowhere to be found.

This is the first thing to know about SDL. Like an address to a
person's house, it doesn't *do* anything on its own. With SDL you are
declaring what you want the final result to be: a schema containing a single
type called ``User``, with a property of type ``str`` called ``name``.

In order for a migration to happen, the EdgeDB server needs to receive
DDL statements telling it what changes to make, in the exact same
way that you give instructions like "turn right at the next intersection"
to your friend who is trying to find your house. In EdgeDB's case,
these commands will start with words like ``create`` and ``drop``
and ``alter`` to tell it what changes to make. EdgeDB accomplishes
these changes by knowing how to turn your declarative SDL into a schema
migration file that contains the DDL statements to accomplish the
necessary changes.

DDL: For computers (mostly)
===========================

To see what a schema migration file looks like, type ``edgedb migration
create``. Now look inside your ``/dbschema/migrations`` folder. You should
see a file called ``00001.esdl`` with the following, our first view into
what DDL looks like.

.. code-block::

    CREATE TYPE default::User {
        CREATE PROPERTY name: std::str;
    };

The declarative schema has now been turned into *imperative* DDL (imperative
meaning "giving orders"), specifically commands telling the database how
to get from the current state to the desired state. Note that, in
contrast to SDL, this code says nothing about the current schema or
its final state. This command would work with the schema of any database
at all that doesn't already have a type called ``User``.

Let's try one more small migration, in which we decide that we don't
want the ``name`` property anymore. Once again, we are declaring the
final state: a ``User`` type with nothing inside. Update your ``default.esdl``
to look like this:

.. code-block:: sdl

    module default {
      type User;
    }

As before, typing ``edgedb migration create`` will create a DDL statement to
change the schema from the current state to the one we have declared. This
time we aren't starting from a blank schema, so the stakes are a bit higher.
After all, dropping a property from a type will also drop all existing data
under that property name. Thus, the schema planner will first ask a question
to confirm the change with us. We will learn a lot more about working with
these questions very soon, but in the meantime just press ``y`` to confirm
the change.

.. code-block::

    db> did you drop property 'name' of object type 'default::User'?
    [y,n,l,c,b,s,q,?]
    > y

Your ``/dbschema/migrations`` folder will now have a new file that contains
the following:

.. code-block::

  ALTER TYPE default::User {
      DROP PROPERTY name;
  };

The difference between SDL and DDL is even clearer this time. The DDL
statement alone doesn't give us any indication what the schema looks like;
all anyone could know from this migration script alone is that there is
a ``User`` type inside a module called ``default`` that *doesn't* have
a property called ``name`` anymore.

.. note::

    EdgeDB commands inside the REPL use a backslash instead of the ``edgedb``
    command, so you can migrate your schema inside the REPL by typing
    ``\migration create`` , followed by ``\migrate``. Not only are the comands
    shorter, but they also execute faster. This is because the database client
    is already connected to your database when you're inside the REPL, which
    is not the case when creating and applying the migration via the CLI.

Order matters in DDL
--------------------

The analogy of a person driving along the road tells us another detail
about DDL: order matters. If you need to first drive two blocks forward
and then turn to the right to reach a destination, that doesn't mean
that you can switch the order around; you can't turn right and *then*
drive two blocks forward and expect to reach the same spot.

Similarly, if you want add a property to an existing type and the
property's type is a new scalar type, the database will need to create
the new scalar type first.

Let's take a look at this by first getting EdgeDB to describe our
schema to us. Typing ``describe schema;`` inside the REPL will display
the following DDL statements:

.. code-block::

  {
    'create module default if not exists;
     create type default::User;',
  }

Thankfully, the DDL statements here are simply the minimum needed
to produce our current schema, not a collection of all the statements
in all of our previous migrations. So while this is a collection of
DDL statements, the DDL produced by ``describe schema`` is just about
as readable as the SDL in your schema.

If we type ``describe schema as sdl;`` then we'll see the SDL version
of the DDL above: a declarative schema as opposed to statements.

.. code-block:: sdl

  module default {
    type User;
  };

Now let's add the new scalar type mentioned above and give it to the
``User`` type. Our schema will now look like this:

.. code-block:: sdl-diff

      module default {
        type User {
    +     name: Name;
        }
    +   scalar type Name extending str;
      }

Note that we are able to define the custom scalar type ``Name`` after we
define the ``User`` type even though we use ``Name`` within that object
because order doesn't matter in SDL. Let's migrate to this new schema
and then use ``describe schema;`` again. You will see the following
statements:

.. code-block::

    create module default if not exists;
    create scalar type default::Name extending std::str;
    create type default::User {
        create property name: default::Name;
    };

The output shows us that the database has gone in the necessary order
to make the schema: first it creates the module, then a scalar type
called ``Name``, and finally the ``User`` type which is now able to
have a property of type ``Name``.

The output with ``describe schema as sdl;`` is also somewhat similar.
It's SDL, but the order matches that of the DDL statements.

.. code-block:: sdl

    module default {
        scalar type Name extending std::str;
        type User {
            property name: default::Name;
        };
    };

Although the schema produced with ``describe schema as sdl;`` may not match
the schema you've written inside ``default.esdl``, it will
show you the order in which statements were needed to reach this final
schema.

Non-interactive migrations
--------------------------

Let's move back to the most basic schema with a single type that
has no properties.

.. code-block:: sdl

    module default {
      type User;
    }

Creating a migration with ``edgedb migration create`` will result
in two questions, one to confirm that we wanted to drop the ``name``
property, and another to drop the ``Name`` type.

.. code-block:: bash

    $ edgedb migration create
    did you drop property 'name' of object type 'default::User'?
    [y,n,l,c,b,s,q,?]
    > y
    did you drop scalar type 'default::Name'? [y,n,l,c,b,s,q,?]
    > y

This didn't take very long, but you can imagine that it could get
annoying if we had decided to drop ten or more types or properties
and had to say yes to every change. In a case like this, we can use
a non-interactive migration. Let's give that a try.

First go into your ``/dbschema/migrations`` folder and delete the
most recent ``.edgeql`` file that drops the property ``name`` and
the scalar type ``Name``. Don't worry - the migration hasn't been
applied yet, so you won't confuse the database by deleting it at this
point. And now type ``edgedb migration create --non-interactive``.

You'll see the same file generated, except that this time there weren't
any questions to answer. A non-interactive migration will work as
long as the database has a high degree of confidence about every change
made, and will fail otherwise.

A non-interactive migration will fail if we make changes to our schema
that are ambiguous. Let's see if we can make a non-interactive migration
fail by doing just that. Delete the most recent ``.edgeql`` migration
file again, and change the schema to the following that only differs by
a single letter. Can you spot the difference?

.. code-block:: sdl

    module default {
      type User {
        nam: Name;
      }
      scalar type Name extending str;
    }

The only difference from the current schema is that we would like
to change the property name ``name`` to ``nam``, but this time EdgeDB isn't
sure what change we wanted to make. Did we intend to:

- Change ``name`` to ``nam`` and keep the existing data?
- Drop ``name`` and create a new property called ``nam``?
- Do something else?

Because of the ambiguity, this non-interactive migration will fail, but with
some pretty helpful output:

.. code-block:: edgeql-repl

    db> \migration create --non-interactive
    EdgeDB intended to apply the following migration:
        ALTER TYPE default::User {
            ALTER PROPERTY name {
                RENAME TO nam;
            };
        };
    But confidence is 0.67, below minimum threshold of 0.99999
    Error executing command: EdgeDB is unable to make a decision.

    Please run in interactive mode to confirm changes, or use
    `--allow-unsafe`

As the output suggests, you can add ``--allow-unsafe`` to a non-interactive
migration if you truly want to push the suggestions through regardless
of the migration tool's confidence, but it's more likely in this case
that you would like to interact with the CLI's questions to help it
make a decision. For example, if we had intended to drop the property
``name`` and create a new property ``nam``, we would simply answer
``n`` when it asks us if we intended to *rename* the property. It
then confirms that we are altering the ``User`` type, and finishes
the migration script.

.. code-block:: edgeql-repl

    db> \migration create
    did you rename property 'name' of object type 'default::User'
    to 'nam'? [y,n,l,c,b,s,q,?]
    > n
    did you alter object type 'default::User'? [y,n,l,c,b,s,q,?]
    > y

Afterwards, you can go into the ``.edgeql`` file that was just created
to confirm that these were the changes we wanted to make. It will
look like this:

.. code-block::

    CREATE MIGRATION m15hu2pbez5od7fe3shlxwcprbqhvctnfavadccjgjszboy26grgka
        ONTO m17m6qjjhtslfkqojvjb4g2vqtzasv5mlbtrqbp6mhwlzv57p5f2uq
    {
      ALTER TYPE default::User {
        CREATE PROPERTY nam: default::Name;
        DROP PROPERTY name;
      };
    };

.. note::

    See the section on
    :ref:`data migrations <ref_migration_guide_migrations_and_hashes>`
    and migration hashes if you are curious about how migrations are named.

This migration will alter the ``User`` type by creating a new property and
dropping the old one. If that is what we wanted, then we can now type
``\migrate`` in the REPL or ``edgedb migrate`` at the command line to complete
the migration.

Questions from the CLI
======================

So far we've only learned how to say "yes" or "no" to the CLI's questions
when we migrate a schema, but quite a few other options are presented
when the CLI asks us a question:

.. code-block::

    did you create object type 'default::PlayerCharacter'? [y,n,l,c,b,s,q,?]
    > y

The choices ``y`` and ``n`` are obviously "yes" and "no," and you can
probably guess that ``?`` will output help for the available response options,
but the others aren't so clear. Let's go over every option to make sure we
understand them.

``y`` (or ``yes``)
------------------

This will accept the proposed change and move on to the next step.
If it's the last proposed change, the migration will now be created.

``n`` (or ``no``)
-----------------

This will reject the proposed change. At this point, the migration
tool will try to suggest a different change if it can, but it won't
always be able to do so.

We can see this behavior with the same tiny schema change we made
above where we changed a property name from ``name`` to ``nam``. In
the output of that ``migration create``, we see the following:

- The CLI first asks us if we renamed the property, to which we say "no".
- It then tries to confirm that we have altered the ``User`` type.
  We say "no" again.
- The CLI then guesses that maybe we are dropping and creating the
  whole ``User`` type instead. This time, we say "yes."
- It then asks us to confirm that we are creating a ``User`` type,
  since we have decided to drop the existing one.

If we say "no" again to the final question, the CLI will throw its hands
up and tell us that it doesn't know what we are trying to do because
there is no way left for it to migrate to the schema that we have
told it to move to.

Here is what that would look like:

.. code-block::

    did you rename property 'name' of object type 'default::User'
    to 'nam'?
    [y,n,l,c,b,s,q,?]
    > n
    did you alter object type 'default::User'? [y,n,l,c,b,s,q,?]
    > n
    did you drop object type 'default::User'? [y,n,l,c,b,s,q,?]
    > y
    did you create object type 'default::User'? [y,n,l,c,b,s,q,?]
    > n
    Error executing command: EdgeDB could not resolve migration with
    the provided answers. Please retry with different answers.

``l`` (or ``list``)
-------------------

This is used to see (list) the actual DDL statements that are being proposed.
When asked the question ``did you alter object type 'default::User'?``
in the example above, we might be wondering exactly what changes will
be made here. How exactly does the database intend to alter the ``User``
type if we say "yes?" Simply pressing ``l`` will show it:

.. code-block::

    The following DDL statements will be applied:
      ALTER TYPE default::User {
          CREATE PROPERTY nam: std::str;
          DROP PROPERTY name;
      };

This shows us clear as day that saying "yes" will result in creating
a new property called ``nam`` and dropping the existing ``name`` property.

So when doubts dwell, press the letter "l!"

``c`` (or ``confirmed``)
------------------------

This simply shows the entire list of statements that have been confirmed.
In other words, this is the migration as it stands at this point.

``b`` (or ``back``)
-------------------

This will undo the last confirmation you agreed to and move you back
a step in the migration. If you haven't confirmed any statements yet,
a message will simply appear to let you know that there is nowhere
further back to move to. So pressing ``b`` will never abort a migration.

The following two keys will stop the migration, but in different ways:

``s`` (or ``stop``)
-------------------

This is also known as a 'split'. Pressing ``s`` will complete the
migration at the current point. Any statements that you have applied
will be applied, but the schema will not yet match the schema in your
``.esdl`` file(s). You can easily start another migration to complete
the remaining changes once you have applied the migration that was
just created. This effectively splits the migration into two or more
files.

``q`` (or ``quit``)
-------------------

Pressing ``q`` will simply quit without saving any of your progress.

.. _ref_migration_guide_migrations_and_hashes:

Data migrations and migration hashes
====================================

Sometimes you may want to initialize a database with some default
data, or add some data to a migration that you have just created before
you apply it.

EdgeDB assumes by default that a migration involves a change to your
schema, so it won't create a migration for you if it doesn't see a
schema change:

.. code-block:: bash

    $ edgedb migration create
    No schema changes detected.

So how do you create a migration with only data? To do this, just
add ``--allow-empty`` to the command:

.. code-block:: bash

    $ edgedb migration create --allow-empty
    Created myproject/dbschema/migrations/00002.edgeql,
    id: m1xseswmheqzxutr55cu66ko4oracannpddujg7gkna2zsjpqm2g3a

You will now see an empty migration in ``dbschema/migrations`` in which you
can enter some queries. It will look something like this:

.. code-block::

    CREATE MIGRATION m1xseswmheqzxutr55cu66ko4oracannpddujg7gkna2zsjpqm2g3a
        ONTO m1n5lfw7n74626cverbjwdhcafnhmbezjhwec2rbt46gh3ztoo7mqa
    {
    };

Let's see what happens if we add some queries inside the braces. Assuming
a schema with a simple ``User`` type, we could then add a bunch of queries
such as the following:

.. code-block::

    CREATE MIGRATION m1xseswmheqzxutr55cu66ko4oracannpddujg7gkna2zsjpqm2g3a
        ONTO m1n5lfw7n74626cverbjwdhcafnhmbezjhwec2rbt46gh3ztoo7mqa
    {
        insert User { name := 'User 1'};
        insert User { name := 'User 2'};
        delete User filter .name = 'User 2';
    };

The problem is, if you save that migration and run ``edgedb migrate``, the CLI
will complain that the migration hash doesn't match what it is supposed to be.
However, it helpfully provides the reason: "Migration names are computed from
the hash of the migration contents."

Fortunately, it also tells you exactly what the hash (the migration name)
will need to be:

.. code-block::

    Error executing command: could not read migrations in
    myproject/dbschema/migrations:

    could not read migration file myproject/dbschema/migrations/00002.edgeql:

    Migration name should be:
    m13g7j2tqu23yaffv6wkn2adp6hayp76su2qtg2lutdh3mmj5xyk6q, but
    m1xseswmheqzxutr55cu66ko4oracannpddujg7gkna2zsjpqm2g3a found instead.


    Migration names are computed from the hash of the migration contents.

    To proceed you must fix the statement to read as:
    CREATE MIGRATION m13g7j2tqu23yaffv6wkn2adp6hayp76su2qtg2lutdh3mmj5xyk6q
    ONTO ...
    Alternatively, revert the changes to the file.

If you change the statement to read in exactly the way the output suggests,
the migration will now work.

That's the manual way to do a data migration, but EdgeDB also has an
``edgedb migration edit`` command that will automate the process for you.
Using ``edgedb migration edit`` will open up the most recent migration for
you to change, and update the migration hash when you close the window.

Aside from exclusive data migrations, you can also create a migration that
combines schema changes *and* data. This is even easier, since it doesn't even
require appending ``--allow-empty`` to the command. Just do the following:

1. Change your schema
2. Type ``edgedb migration create`` and respond to the CLI's questions
3. Add your queries to the file (best done on the bottom after the
   DDL statements have changed the schema) either manually or using
   ``edgedb migration edit``
4. Type ``edgedb migrate`` to migrate the schema. If you have changed the
   schema file manually, copy the suggested name into the migration hash
   and type ``edgedb migrate`` again.

The `EdgeDB tutorial </tutorial>`_ is a good example of a database
set up with both a schema migration and a data migration. Setting
up a database with `schema changes in one file and default data in
a second file <tutorial_files_>`_ is a nice way to separate the two operations
and maintain high readability at the same time.

Squashing migrations
====================

Users often end up making many changes to their schema because
of how effortless it is to do. (And in the next section we will learn
about ``edgedb watch``, which is even more effortless!) This leads to
an interesting side effect: lots of ``.edgeql`` files, many of which
represent trials and approaches that don't end up making it to the
final schema.

Once you are done, you might want to squash the migrations into a
single file. This is especially nice if you need to frequently initialize
database instances using the same schema, because all migrations are
applied when an instance starts up. You can imagine that the output
would be pretty long if you had dozens and dozens of migration files
to work through:

.. code-block::

    Initializing EdgeDB instance...
    Applying migrations...
    Applied m13brvdizqpva6icpcvmsc3fee2yt5j267uba6jugy6iugcbs2djkq
    (00001.edgeql)
    Applied m1aildofb3gvhv3jaa5vjlre4pe26locxevqok4semmlgqwu3xayaa
    (00002.edgeql)
    Applied m1ixxlsdgrlinfijnrbmxdicmpfav33snidudqi7fu4yfhg4nngoza
    (00003.edgeql)
    Applied m1tsi4amrdbcfjypu72duyckrlvvyb46r3wybd7qnbmem4rjvnbcla
    (00004.edgeql)
    ...and so on...
    Project initialized.

To squash your migrations, just run ``edgedb migration create`` with the
``--squash`` option. Running this command will first display some helpful
info to keep in mind before committing to the operation:

.. code-block::

    Current database revision is:
    m16ixoukn7ulqdn7tp6lvx2754hviopanufv2lm6wf4x2borgc3g6a
    While squashing migrations is non-destructive,
    it may lead to manual work if done incorrectly.

    Items to check before using --squash:
    1. Ensure that `./dbschema` dir is comitted
    2. Ensure that other users of the database have the revision
    above or can create database from scratch.
        To check a specific instance, run:
        edgedb -I <name> migration log --from-db --newest-first --limit 1
    1. Merge version control branches that contain schema changes
    if possible.

    Proceed? [y/n]

Press ``y`` to squash all of your existing migrations into
a single file.

Fixups during a squash
----------------------

If your schema doesn't match the schema in the database, EdgeDB will
prompt you to create a *fixup* file, which can be useful to, as the CLI
says, "automate upgrading other instances to a squashed revision".
You'll see fixups inside ``/dbschema/fixups``. Their file names
are extremely long because they are simply two migration hashes joined
together by a dash. This means a fixup that begins with

.. code-block::

    CREATE MIGRATION
    m1v3vqmwif4ml3ucbzi555mjgm4myxs2husqemopo2sz2m7otr22ka
    ONTO m16awk2tzhtbupjrzoc4fikgw5okxpfnaazupb6rxudxwin2qfgy5q

will have a file name a full 116 characters in length.

The CLI output when using squash along with a fixup is pretty informative
on its own, so let's just walk through the output as you'll see it
in practice. First we'll begin with this schema:

.. code-block:: sdl

  type User {
    name: str;
  }

Then remove ``name: str;`` from the ``User`` type, migrate, put it back
again, and migrate. You can repeat this as many times as you like.
One quick way to "remove" items from your schema that you might want
to restore later is to simply use a ``#`` to comment out the entire line:

.. code-block:: sdl

  type User {
   # name: str;
  }

After a few of these simple migrations, you'll now have multiple files
in your ``/migrations`` folder — none of which were all that useful — and
may be in the mood to squash them into one.

Next, change to this schema **without migrating it**:

.. code-block:: sdl

  type User {
    name: str;
    nickname: str;
  }

Now run ``edgedb migration create --squash``. The output is first
the same as with our previous squash:

.. code-block:: bash

    $ edgedb migration create --squash
    Current database revision:
    m16awk2tzhtbupjrzoc4fikgw5okxpfnaazupb6rxudxwin2qfgy5q
    While squashing migrations is non-destructive,
    it may lead to manual work if done incorrectly.

    Items to check before using --squash:
    1. Ensure that `./dbschema` dir is comitted
    2. Ensure that other users of the database have the revision
    above or can create database from scratch.
        To check a specific instance, run:
        edgedb -I <name> migration log --from-db --newest-first --limit 1
    3. Merge version control branches that contain schema changes
    if possible.

    Proceed? [y/n]
    > y

But after typing ``y``, the CLI will notice that the existing schema
differs from what you have and offers to make a fixup file:

.. code-block::

    Your schema differs from the last revision.
    A fixup file can be created
    to automate upgrading other instances to a squashed revision.
    This starts the usual migration creation process.

    Feel free to skip this step if you don't have
    other instances to migrate

    Create a fixup file? [y/n]
    > y

You will then see the the same questions that would otherwise show up in
a standard migration:

.. code-block::

    db> did you create property 'nickname' of object type 'default::User'?
    [y,n,l,c,b,s,q,?]
    > y
    Squash is complete.

Finally, the CLI will give some advice on recommended commands when
working with git after doing a squash with a fixup.

.. code-block::

    Remember to commit the `dbschema` directory including deleted files
    and `fixups` subdirectory. Recommended command:
        git add dbschema

    The normal migration process will update your migration history:
        edgedb migrate

We'll take its suggestion to apply the migration:

.. code-block:: bash

    $ edgedb migrate

    Applied m1v3vqmwif4ml3ucbzi555mjgm4myxs2husqemopo2sz2m7otr22ka
    (m16awk2tzhtbupjrzoc4fikgw5okxpfnaazupb6rxudxwin2qfgy5q-
    m1oih6aevfcftysukvofwuth2bsuj5aahkdnpabscry7p7ljkgbxma.edgeql)


.. note::

    Squashing is limited to schema changes, so queries inside
    data migrations will be discarded during a squash.

EdgeDB Watch
============

Another option when quickly iterating over schema changes is ``edgedb watch``.
This will create a long-running process that keeps track of every time you
save an ``.esdl`` file inside your ``/migrations`` folder, letting you know
if your changes have successfully compiled or not. The ``edgedb watch``
command itself will show the following input when the process starts up:

.. code-block::

    Connecting to EdgeDB instance 'anything' at localhost:10700...
    EdgeDB Watch initialized.
    Hint: Use `edgedb migration create` and `edgedb migrate --dev-mode`
    to apply changes once done.
    Monitoring "/home/instancename".

Unseen to the user, ``edgedb watch`` will begin creating individual migration
scripts for every time you save a change to one of your files. These
are stored as separate "dev mode" migrations, which are sort of like
preliminary migrations that haven't been turned into a standalone
migration script yet.

We can test this out by starting with this schema:

.. code-block:: sdl

    module default {
      type User {
        name: str;
      }
    }

Now let's add a single property. Keep an eye on your terminal output and
hit after making a change to the following schema:

.. code-block:: sdl

    module default {
      type User {
        name: str;
        number: int32;
      }
    }

You will see a quick "calculating diff" show up as ``edgedb watch`` checks
to see that the change we made was a valid one. As the change we made was
to a valid schema, the "calculating diff" message will disappear pretty
quickly.

However, if the schema file you save is incorrect, the output will be a lot
more verbose. Let's add some incorrect syntax to the existing schema:

.. code-block:: sdl

    module default {
      type User {
        name: str;
        number: int32;
        wrong_property: i32; # Should say int32, not i32
      }
    }

Once you hit save, ``edgedb watch`` will suddenly pipe up and inform you
that the schema can't be resolved:

.. code-block::

    error: type 'default::i32' does not exist
    ┌─ myproject/dbschema/default.esdl:5:25
    │
    5 │         wrong_property: i32;
    │                         ^^^ error

    Schema migration error:
    cannot proceed until .esdl files are fixed

Once you correct the ``i32`` type to ``int32``, you will see a message
letting you know that things are okay now.

.. code-block::

    Resolved. Schema is up to date now.

The process will once again quieten down, but will continue to watch your
schema and apply migrations to any changes you make to your schema.

``edgedb watch`` is best run in a separate instance of your command line so
that you can take care of other tasks — including officially migrating
when you are satisfied with your current schema — without having to
stop the process.

If you are curious what is happening as ``edgedb watch`` does its thing,
try the following query after you have made some changes. It will return
a few lists of applied migrations, grouped by the way they were generated.

.. code-block::

    group schema::Migration {
        name,
        script
    } by .generated_by;

Some migrations will contain nothing in their ``generated_by`` property,
while those generated by ``edgedb watch`` will have a
``MigrationGeneratedBy.DevMode``.

.. note::

    The final option (aside from ``DevMode`` and the empty set) for
    ``generated_by`` is ``MigrationGeneratedBy.DDLStatement``, which will
    show up if you directly change your schema by using DDL, which is
    generally not recommended.

Once you are satisfied with your changes while running ``edgedb watch``,
just create the migration with ``edgedb migration create`` and then
apply them with one small tweak to the ``migrate`` command:
``edgedb migrate --dev-mode`` to let the CLI know to apply the migrations
made during dev mode that were made by ``edgedb watch``.

Branches
========

EdgeDB's branches can be a useful part of your schema migrations, especially
when you're developing new features or prototyping experimental features. By
creating a new branch, you can isolate schema changes from your other branches.

Imagine a scenario in which your main branch is called ``main`` (which is the
default name for the initial branch) and your feature branch is called
``feature``. This is the ideal workflow for using an EdgeDB branch alongside a feature
branch in your VCS to develop a new feature:

1. Create a new feature branch with :ref:`ref_cli_edgedb_branch_create`
2. Build your feature
3. Pull any changes on ``main``
4. Rebase your feature branch on ``main`` with
   :ref:`ref_cli_edgedb_branch_rebase`
5. Merge ``feature`` onto ``main`` with :ref:`ref_cli_edgedb_branch_merge`

The workflow is outlined in detail in :ref:`the branches guide in our "Get
started" section <ref_intro_branches>`.

.. _ref_migration_guide_branches_rebasing:

How rebasing works
------------------

Rebasing the branch in step 4 above happens with a single command — ``edgedb
branch rebase main`` — but that one command has quite a bit going on under the
hood. Here's how it works:

1. The CLI first clones the ``main`` branch with the data into a ``temp``
   branch.
2. It introspects the migration histories of ``temp`` and the ``feature``
   branch to establish where they diverge.
3. It applies all the divergent migrations from the ``feature`` branch
   on the ``temp`` branch.
4. If the operation is successful, it drops the ``feature``
   branch and renames ``temp`` to ``feature``.

With the deceptively complicated rebase completed with just that single
command, you've stacked the dominoes perfectly for your merge to succeed!


So, you really want to use DDL?
===============================

You might have a good reason to use a direct DDL statement or two
to change your schema. How do you make that happen? EdgeDB disables
the usage of DDL by default if you have already carried out a migration
through the recommended migration commands, so this attempt to use DDL
will not work:

.. code-block:: edgeql-repl

    db> create type MyType;
    error: QueryError: bare DDL statements are not
    allowed on this database branch
    ┌─ <query>:1:1
    │
    1 │ create type MyType;
    │ ^^^^^^^^^^^^^^^^^^ Use the migration commands instead.
    │
    = The `allow_bare_ddl` configuration variable is set to
    'NeverAllow'.  The `edgedb migrate` command normally sets
    this to avoid accidental schema changes outside of the
    migration flow.

This configuration can be overridden by the following command which
changes the enum ``allow_bare_ddl`` from the default ``NeverAllow``
to the other option, ``AlwaysAllow``.

.. code-block:: edgeql-repl

    db> configure current branch set allow_bare_ddl := 'AlwaysAllow';

Note that the command is ``configure current branch`` and not ``configure
instance``, as ``allow_bare_ddl`` is evaluated on the branch level.

That wasn't so bad, so why did the CLI tell us to try to "avoid accidental
schema changes outside of the migration flow?" Why is DDL disabled
after running a migration in the first place?

So, you really wanted to use DDL but now regret it?
===================================================

Let's start out with a very simple schema to see what happens after
DDL is used to directly modify a schema.

.. code-block:: sdl

    module default {
      type User {
          name: str;
      }
    }

Next, we'll set the current branch to allow bare DDL:

.. code-block:: edgeql-repl

    db> configure current branch set allow_bare_ddl := 'AlwaysAllow';

And then create a type called ``SomeType`` without any properties:

.. code-block:: edgeql-repl

    db> create type SomeType;
    OK: CREATE TYPE

Your schema now contains this type, as you can see by typing ``describe
schema`` or ``describe schema as sdl``:

.. code-block::

    {
    'module default {
        type SomeType;
        type User {
            property name: std::str;
        };
    };',
    }

Great! This type is now inside your schema and you can do whatever
you like with it.

But this has also ruined the migration flow. Watch what happens when
you try to apply the change:

.. code-block:: edgeql-repl

    db> \migration create
    Error executing command: Database must be updated to
    the last migration on the filesystem for
    `migration create`. Run:
    edgedb migrate

    db> \migrate
    Error executing command: database applied migration
    history is ahead of migration history in
    "myproject/dbschema/migrations" by 1 revision

Sneakily adding ``SomeType`` into your schema to match won't work
either. The problem is that there *is* a migration already present,
it just doesn't exist inside your ``/migrations`` folder. You can
see it with the following query:

.. code-block:: edgeql-repl

    db> select schema::Migration {*}
    ...  filter
    ...  .generated_by = schema::MigrationGeneratedBy.DDLStatement;
    {
    schema::Migration {
        id: 3882894a-8bb7-11ee-b009-ad814ec6a5f5,
        name: 'm1s6oniru3zqepiaxeljt7vcgyynxuwh4ki3zdfr4hfavjozsndfua',
        internal: false,
        builtin: false,
        computed_fields: [],
        script: 'SET generated_by :=
            (schema::MigrationGeneratedBy.DDLStatement);
    CREATE TYPE SomeType;',
        message: {},
        generated_by: DDLStatement,
    },
    }

Fortunately, the fix is not too hard: we can use the command
``edgedb migration extract``. This command will retrieve the migration(s)
created using DDL and assign each of them a proper file name and hash
inside the ``/dbschema/migrations`` folder, effectively giving them a proper
position inside the migration flow.

Note that at this point your ``.esdl`` schema will still not match
the database schema, so if you were to type ``edgedb migration create``
the CLI would then ask you if you want to drop the type that you just
created - because it doesn't exist inside there. So be sure to change
your schema to match the schema inside the database that you have
manually changed via DDL. If in doubt, use ``describe schema as sdl``
to compare or use ``edgedb migration create`` and check the output.
If the CLI is asking you if you want to drop a type, that means that
you forgot to add it to the schema inside your ``.esdl`` file(s).


Multiple migrations to keep data
================================

Sometimes you may want to change your schema in a complex way that doesn't
allow you to keep existing data. For example, what if you decide that you
don't need a ``multi`` link anymore but would like to keep some of the
information in the currently linked to objects as an array instead? One
way to make this happen is by migrating more than once.

Let's give this a try by starting with with a simple ``User`` type that has
a ``friends`` link to other ``User`` objects. (If you've been following along
all this time, a quick migration to this schema will be a breeze.)

.. code-block:: sdl

    module default {
      type User {
          name: str;
          multi friends: User;
      }
    }

First let's insert three ``User`` objects, followed by an update to
make each ``User`` friends with all of the others:

.. code-block:: edgeql-repl

    db> insert User {
    ... name := 'User 1'
    ... };
    {default::User {id: d44a19bc-8bc1-11ee-8f28-47d7ec5238fe}}
    db> insert User {
    ... name := 'User 2'
    ... };
    {default::User {id: d5f941c0-8bc1-11ee-8f28-b3f56009a7b0}}
    db> insert User {
    ... name := 'User 3'
    ... };
    {default::User {id: d79cb03e-8bc1-11ee-8f28-43fe3f68004c}}
    db> update User set {
    ...    friends := (select detached User filter User.name != .name)
    ...  };

Now what happens if we now want to change ``multi friends`` to an
``array<str>``? If we were simply changing a scalar property to another
property it would be easy, because EdgeDB would prompt us for a conversion
expression, but a change from a link to a property is different:

.. code-block:: sdl

    module default {
      type User {
          name: str;
          multi friends: array<str>;
      }
    }

Doing a migration as such will just drop the ``friends`` link (along
with its data) and create a new ``friends`` property - without any
data at all.

To solve this problem, we can do two migrations instead of one. First
we will keep the ``friends`` link, while adding a new property called
``friend_names``:

.. code-block:: sdl

    module default {
      type User {
        name: str;
        multi friends: User;
        friend_names: array<str>;
      }
    }

Upon using ``edgedb migration create``, the CLI will simply ask us if we
created a property called ``friend_names``. We haven't applied the migration
yet, so we might as well put the data inside the same migration. A simple
``update`` will do the job! As we learned previously,
``edgedb migration edit`` is the easiest way to add data to a migration. Or
you can manually add the ``update``, try to apply the migration, and change
the migration hash to the output suggested by the CLI.

.. code-block::

    CREATE MIGRATION m1hvciatdgpo3a74wagbmwhbunxbridda4qvdbrr3z2a34opks63rq
        ONTO m1vktopcva7l6spiinh5e5nnc4dtje4ygw2fhismbmczbyaqbws7jq
    {
    ALTER TYPE default::User {
        CREATE PROPERTY friend_names: array<std::str>;
    };
    update User set { friend_names := array_agg(.friends.name) };
    };

Once the migration is applied, we can do a query to confirm that the data
inside ``.friends.name`` when converted to an array is indeed the same as
the data inside the ``friend_names`` property:

.. code-block:: edgeql-repl

    db> select User { f:= array_agg(.friends.name), friend_names };
    {
    default::User {
      f: ['User 2', 'User 3'],
      friend_names: ['User 2', 'User 3']
      },
    default::User {
      f: ['User 1', 'User 3'],
      friend_names: ['User 1', 'User 3']
      },
    default::User {
      f: ['User 1', 'User 2'],
      friend_names: ['User 1', 'User 2']
      },
    }

Or we could also use the ``all()`` function to confirm that this is the case.

.. code-block:: edgeql-repl

    db> select all(array_agg(User.friends.name) = User.friend_names);
    {true}

Looks good! And now we can simply remove ``multi friends: User;``
from our schema and do a final migration.

Migration internals
===================

We've now reached the most optional part of the migrations tutorial,
but an interesting one for those curious about what goes on behind
the scenes during a migration.

Migrations in EdgeDB before the advent of the ``edgedb project`` flow
were still automated but required more manual work if you didn't
want to accept all of the suggestions provided by the server. This
process is in fact still used to migrate even today; the CLI just
facilitates it by making it easy to respond to the generated suggestions.

:ref:`Early EdgeDB migrations took place inside a transaction <ref_eql_ddl_migrations>`
handled by the user that essentially went like this:

.. code-block::

    db> start migration to { <your schema goes here> };

This starts the migration, after which the quickest process was to
type ``populate migration`` to accept the statements suggested by
the server, and then ``commit migration`` to finish the process.

Now, there is another option besides simply typing ``populate migration``
that allows you to look at and handle the suggestions every step of
the way (in the same way the CLI does today), and this is what we
are going to have some fun with. You can see
`the original migrations RFC <rfc_>`_ if you are curious.

It is *very* finicky compared to the CLI, resulting in a failed transaction
if any step along the way is different from the expected behavior,
but is an entertaining challenge to try to get right if you want to
truly understand how migrations work in EdgeDB.

This process requires looking at the server's proposed solutions every
step of the way, and these steps are best seen in JSON format. We can make
this format as readable as possible with the following command:

.. code-block:: edgeql-repl

    db> \set output-format json-pretty

First, let's begin with the same same simple schema used in the previous
examples, via the regular ``edgedb migration create`` and ``edgedb migrate``
commands.

.. code-block:: sdl

    module default {
      type User {
        name: str;
      }
    }

And, as before, we will make a somewhat ambiguous change by changing
``name`` to ``nam``.

.. code-block:: sdl

    module default {
      type User {
        nam: str;
      }
    }

And now it's time to give the older migration method a try! To move to this
schema using the old method, we will need to start a migration by pasting our
desired schema into a ``start migration to {};`` block:

.. code-block:: edgeql-repl

    db> start migration to {
    ...   module default {
    ...     type User {
    ...       nam: str;
    ...     }
    ...   }
    ... };

You should get the output ``OK: START MIGRATION``, followed by a prompt
that ends with ``[tx]`` to show that we are inside of a transaction.
Anything we do here will have no effect on the current registered
schema until we finally commit the migration.

So now what do we do? We could simply type ``populate migration``
to accept the server's suggested changes, but let's instead take a
look at them one step at a time. To see the current described change,
type ``describe current migration as json;``. This will generate the
following output:

.. code-block::

    {
    "parent": "m14opov4ymcbd34x7csurz3mu4u6sik3r7dosz32gist6kpayhdg4q",
    "complete": false,
    "proposed": {
    "prompt": "did you rename property 'name' of object type 'default::User'
        to 'nam'?",
    "data_safe": true,
    "prompt_id": "RenameProperty PROPERTY default::__|name@default|User
        TO default::__|nam@default|User",
    "confidence": 0.67,
    "statements": [{"text": "ALTER TYPE default::User {\n    ALTER
        PROPERTY name {\n        RENAME TO nam;\n    };\n};"}],
    "required_user_input": []
    },
    "confirmed": []
    }

The server is telling us with ``"complete": false`` that this suggestion
is not the final step in the migration, that it is 67% confident that
its suggestion is correct, and that we should probably type the following
statement:

.. code-block::

    ALTER TYPE default::User { ALTER PROPERTY name { RENAME TO nam; };};

Don't forget to remove the newlines (``\n``) from inside the original
suggestion; the transaction will fail if you don't take them out. If the
migration fails at any step, you will see ``[tx]`` change to ``[tx:failed]``
and you will have to type ``abort migration`` to leave the transaction
and begin the migration again.

Technically, at this point you are permitted to write any DDL statement
you like and the migration tool will adapt its suggestions to reach
the desired schema. Doing so though is bad practice and is more than likely
to generate an error when you try to commit the migration.
(Even so, give it a try if you're curious.)

Let's dutifully type the suggested statement above, and then use
``describe current migration as json`` again to see what the current
status of the migration is. This time we see two major differences:
"complete" is now ``true``, meaning that we are at the end of the
proposed migration, and "proposed" does not contain anything. We can
also see our confirmed statement inside "confirmed" at the bottom.

.. code-block::

    {
    "parent": "m1fgpuxbvd74m6pb72rdikakjv3fv7cftrez7r56qjgonboimp5zoa",
    "complete": true,
    "proposed": null,
    "confirmed": ["ALTER TYPE default::User {\n ALTER PROPERTY name
    {\n RENAME TO nam;\n };\n};"]
    }

With this done, you can commit the migration and the migration
will be complete.

.. code-block:: edgeql-repl

    db[tx]> commit migration;
    OK: COMMIT MIGRATION

Since this migration was created using direct DDL statements,
you will need to use ``edgedb migration extract`` to extract the latest
migration and give it a proper ``.edgeql`` file in the same way we
did above in the "So you really wanted to use DDL but now regret it?"
section.

.. _rfc: https://github.com/edgedb/rfcs/blob/master/text/1000-migrations.rst
.. _tutorial_files: https://github.com/edgedb/website/tree/main/content/tutorial/dbschema/migrations
