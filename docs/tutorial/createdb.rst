.. _ref_tutorial_createdb:

2. Database and Schema
======================

.. note::

    Syntax-highlighter packages are available for
    `Atom <https://atom.io/packages/edgedb>`_,
    `Visual Studio Code <https://marketplace.visualstudio.com/
    itemdetails?itemName=magicstack.edgedb>`_,
    `Sublime Text <https://packagecontrol.io/packages/EdgeDB>`_,
    and `Vim <https://github.com/edgedb/edgedb-vim>`_.


Once the EdgeDB server has been :ref:`installed
<ref_tutorial_install>` on the system, it's time to create the first
EdgeDB project.  We'll call it "tutorial". Create a project directory
called ``tutorial`` and run the following command from within in:

.. code-block:: bash

    $ edgedb project init

Follow the prompts, accepting the default suggestions. At the end of
the process you'll have a ``tutotial`` EdgeDB instance created and
ready to use.

As long as you're running the commands from inside the project
directory, you can start EdgeDB REPL by simply running:

.. code-block:: bash

    $ edgedb

Now we need to set up the schema. Let's create a schema for a movie
database. It will have 2 types of objects: movies and people who
directed and acted in them.


.. _ref_tutorial_createdb_sdl:

SDL
---

The recommended way to manage the database schema is by using the
:ref:`EdgeDB schema definition language <ref_eql_sdl>` (or SDL). It
provides a way to describe a :ref:`migration
<ref_eql_ddl_migrations>` to a specific schema state. It is great
for setting up a new database because it focuses on expressing the
final :ref:`types <ref_eql_sdl_object_types>` and their
:ref:`relationships <ref_eql_sdl_links>` without worrying about
the order of the definitions. This is also the format that the
EdgeDB built-in migration tools are designed to use.

The project initialization script should have created ``dbschema``
directory. That's where the schema and migrations files will reside.
There's already an empty schema file in place that we will use for the
tutorial. Using an editor of your choice add the following content to
``dbschema/default.esdl``:

.. code-block:: sdl

    module default {
        type Person {
            required property first_name -> str;
            required property last_name -> str;
        }
        type Movie {
            required property title -> str;
            # the year of release
            property year -> int64;
            required link director -> Person;
            multi link actors -> Person;
        }
    };

Now we're all set to run the very first migration to apply the schema
to the database. The built-in migration tool will ask a series of
questions to make sure that EdgeDB correctly inferred the changes:

.. code-block:: bash

    $ edgedb -I tutorial migration create
    did you create object type 'default::Person'? [y,n,l,c,b,s,q,?]
    ?

    y - confirm the prompt, use the DDL statements
    n - reject the prompt
    l - list the DDL statements associated with prompt
    c - list already confirmed EdgeQL statements
    b - revert back to previous save point, perhaps previous question
    s - stop and save changes (splits migration into multiple)
    q - quit without saving changes
    h or ? - print help
    did you create object type 'default::Person'? [y,n,l,c,b,s,q,?]
    y
    did you create object type 'default::Movie'? [y,n,l,c,b,s,q,?]
    y
    Created ./dbschema/migrations/00001.edgeql, id:
    m1la5u4qi33nsrhorvl6u7zdiiuvrx6y647mhk3c7suj7ex5jx5ija

Before moving on to the next step let's unpack what just happened.
The migration tool is asking whether new objects were added to the
schema, which is what we expect for a brand new schema, so we can
respond with ``y`` and proceed. Now that we have accepted all the
changes for the migration a new file was added to our ``dbschema``
directory: ``dbschema/migrations/00001.edgeql``. It contains all
the DDL commands necessary for the migration. Now we can apply it to
the database:

.. code-block:: bash

    $ edgedb migrate
    Applied m1la5u4qi33nsrhorvl6u7zdiiuvrx6y647mhk3c7suj7ex5jx5ija
    (00001.edgeql)

Now that the schema is set up we're ready to
:ref:`populate the database with data <ref_tutorial_queries>`.
