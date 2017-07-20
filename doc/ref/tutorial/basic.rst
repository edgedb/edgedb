Sample application
==================

This tutorial covers the setup and a few use case for an issue
management platform.

Setup the database
------------------

Install. Run the server.

Once the EdgeDB server is up and running the first thing to do is to
add a schema that we will be using. To do that, let's consider which
objects we will need in our system. Obviously we want a ``User`` and
an ``Issue``. We probably want a ``Status``, too. In order to provide
feedback on issues a ``Comment`` concept seems like a good idea. So
let's start with defining the schema with these 4 concepts. Later on,
if we need more, we can amend the schema (producing a
:ref:`migration <ref_schema_evolution>` to alter the database).

The original schema would be something like this:

.. code-block:: eschema

    concept User:
        required link name to str

    concept Issue:
        required link text to str
        required link status to Status
        required link owner to User

    concept Status:
        required link name to str:
            # the status names should be unique
            constraint unique

    concept Comment:
        required link text to str
        # It makes more sense to link comments to issues rather than
        # vice-versa, since that makes their coupling in the schema
        # less tight. This is a good practice for relationships that
        # don't represent inherent properties.
        required link issue to Issue:
            mapping := '*1'
        link timestamp to datetime:
            default := SELECT datetime::current_datetime()
            # the timestap will be automatically set to the current
            # time if it is not specified at the point of comment
            # creation

The schema can be applied either via a migration tool or directly
using ``CREATE MIGRATION`` and ``COMMIT MIGRATION`` commands. Let's do it in
the interactive console via the low level EdgeQL commands.

.. code-block:: eql

    CREATE MODULE example;

    CREATE MIGRATION example::d1 TO eschema $$
    concept User:
        required link name to str

    concept Issue:
        required link text to str
        required link status to Status
        required link owner to User

    concept Status:
        required link name to str:
            # the status names should be unique
            constraint unique

    concept Comment:
        required link text to str
        # It makes more sense to link comments to issues rather than
        # vice-versa, since that makes their coupling in the schema
        # less tight. This is a good practice for relationships that
        # don't represent inherent properties.
        required link issue to Issue:
            mapping := '*1'
        link timestamp to datetime:
            default := SELECT datetime::current_datetime()
            # the timestap will be automatically set to the current
            # time if it is not specified at the point of comment
            # creation
    $$;

    COMMIT MIGRATION example::d1;

Now we can start populating the DB with actual objects. For
consistency with examples in other parts of the documentation let's
name the module "example".

Let's start with a few users and status objects:

.. code-block:: eql

    INSERT example::User {
        name := 'Alice Smith'
    };

    INSERT example::User {
        name := 'Bob Johnson'
    };

    INSERT example::Status {
        name := 'Open'
    };

    INSERT example::Status {
        name := 'Closed'
    };

Note that alternatively, the users and statuses could have been created using
:ref:`GraphQL queries <ref_graphql_overview>`.

Now that we have the basics set up, we can log the first issue:

.. code-block:: eql

    WITH MODULE example
    INSERT Issue {
        text :=
            'The issue system needs more status values and maybe priority.',
        status := (SELECT Status FILTER Status.name = 'Open'),
        owner := (SELECT User FILTER User.name = 'Bob Johnson')
    };

Let's add priority to the schema, first. We'll have one new
``concept`` and a change to the existing ``Issue``:

.. code-block:: eschema

    concept User:
        required link name to str

    concept Status:
        required link name to str:
            # the status names should be unique
            constraint unique

    concept Comment:
        required link text to str
        # It makes more sense to link comments to issues rather than
        # vice-versa, since that makes their coupling in the schema
        # less tight. This is a good practice for relationships that
        # don't represent inherent properties.
        required link issue to Issue:
            mapping := '*1'
        link timestamp to datetime:
            default := SELECT datetime::current_datetime()
            # the timestap will be automatically set to the current
            # time if it is not specified at the point of comment
            # creation

    #
    # no changes to the above concepts
    #

    concept Issue:
        required link text to str
        required link status to Status
        required link owner to User
        link priority to Priority
        # let's make priority optional

    concept Priority:
        required link name to str:
            constraint unique

.. code-block:: eql

    CREATE MIGRATION example::d2
    FROM example::d1
    TO eschema $$
        # ... new schema goes here
    $$;

    COMMIT MIGRATION example::d2;

Given the new schema we can use the migration tools to apply the
changes to our existing EdgeDB data. After that we can create
``Status`` and ``Priority`` objects.

.. code-block:: eql

    INSERT example::Priority {
        name := 'High'
    };

    INSERT example::Priority {
        name := 'Low'
    };

    INSERT example::Status {
        name := 'New'
    };

    INSERT example::Status {
        name := 'Rejected'
    };

With the priority objects all set up we can now update the ``Issue``
to have "High" priority.

.. code-block:: eql

    WITH MODULE example
    UPDATE Issue
    FILTER Issue.id = 'd54f6472-8f07-44d9-909e-22864dc6f811'
    SET {
        priority := (SELECT Priority FILTER Priority.name = 'High')
    };

    # The id used above is something that would have been returned by
    # the 'INSERT Issue ...' query or we could simply query it
    # separately.

It seems though that the issue has actually been resolved, so let's
make a comment about that and close the issue.

.. code-block:: eql

    WITH MODULE example
    INSERT Comment {
        issue := (
            SELECT Issue
            FILTER Issue.id = 'd54f6472-8f07-44d9-909e-22864dc6f811'
        ),
        text := "I've added more statuses and created priorities."
    };

    WITH MODULE example
    UPDATE Issue
    SET {
        status := (SELECT Status FILTER Status.name = 'Closed')
    };

At this point we may have realized that ``Issue`` and ``Comment`` have
some underlying similarity, they are both pieces of text written by
some user. Moreover, we could envision that as the system grows we
could have other concepts that are owned by users as well as other
kinds of text objects that record messages and such. While we're at
it, we might as well also create an abstract concept for things with a
``name``. So let's update the schema again, this time mostly
refactoring.

.. code-block:: eschema

    abstract concept Named:
        required link name to str

    # Dictionary is a NamedObject variant, that enforces
    # name uniqueness across all instances if its subclass.
    abstract concept Dictionary extending Named:
        required link name to str:
            abstract constraint unique

    abstract concept Text:
        # This is an abstract object containing text.
        required link text to str:
            # let's limit the maximum length of text to 10000
            # characters.
            constraint maxlength(10000)

    abstract concept Owned:
        # don't make the link owner required so that we can first
        # assign an owner to Comment objects already in the DB
        link owner to User:
            mapping := '*1'

    concept User extending Named
    # no need to specify 'link name' here anymore as it's inherited

    concept Issue extending Text, Owned:
        required link status to Status
        link priority to Priority
        required link owner to User:
            mapping := '*1'
        # because we override the link owner to be required,
        # we need to keep this definition

    concept Priority extending Dictionary

    concept Status extending Dictionary

    concept Comment extending Text, Owned:
        required link issue to Issue:
            mapping := '*1'
        link timestamp to datetime:
            default := SELECT datetime::current_datetime()
            # the timestap will be automatically set to the current
            # time if it is not specified at the point of comment
            # creation

.. code-block:: eql

    CREATE MIGRATION example::d3
    FROM example::d2 TO eschema $$
        # ... new schema goes here
    $$;
    COMMIT MIGRATION example::d3;

After the migration we still need to fix all comments in our system to
have some owner. In the example so far there was only comment but
let's treat it as if we have several comments made by the same person.

.. code-block:: eql

    WITH MODULE example
    UPDATE Comment
    SET {
        owner := (SELECT User FILTER User.name = 'Alice Smith')
    };

Now that all of the comments have an owner we can further update the
schema to make owner a required field for all ``Owned`` objects.

.. code-block:: eschema

    abstract concept Named:
        required link name to str

    # Dictionary is a NamedObject variant, that enforces
    # name uniqueness across all instances if its subclass.
    abstract concept Dictionary extending Named:
        required link name to str:
            abstract constraint unique

    abstract concept Text:
        # This is an abstract object containing text.
        required link text to str:
            # let's limit the maximum length of text to 10000
            # characters.
            constraint maxlength(10000)

    concept User extending Named
    # no need to specify 'link name' here anymore as it's inherited

    concept Priority extending Dictionary

    concept Status extending Dictionary

    concept Comment extending Text, Owned:
        required link issue to Issue:
            mapping := '*1'
        link timestamp to datetime:
            default := SELECT datetime::current_datetime()
            # the timestap will be automatically set to the current
            # time if it is not specified at the point of comment
            # creation

    #
    # just as before, no changes to the above concepts
    #

    abstract concept Owned:
        # don't make the link owner required so that we can first
        # assign an owner to Comment objects already in the DB
        required link owner to User:
            mapping := '*1'

    concept Issue extending Text, Owned:
        required link status to Status
        link priority to Priority
        # notice we no longer need to override the owner link

.. code-block:: eql

    CREATE MIGRATION example::d4
    FROM example::d3
    TO eschema $$
        # ... new schema goes here
    $$;
    COMMIT MIGRATION example::d4;

After several schema migrations and even a data migration we have
arrived at a state with reasonable amount of features for our issue
tracker EdgeDB backend. Now let's log a few more issues and run some
queries to analyze them.


Use cases
---------

Let's consider some of the possible interactions with the issue
tracker system, using both EdgeQL and GraphQL.

.. todo::

    needs more content

Analytics
---------

For running complex queries native EdgeQL is better suited than GraphQL.

.. todo::

    needs more content
