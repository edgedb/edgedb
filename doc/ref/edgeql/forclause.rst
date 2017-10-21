.. _ref_edgeql_forclause:


Usage of FOR clause
===================

.. note::

    This section needs a complete re-write.

A ``FOR`` clause provides a shorthand for multiple repetitive
statements the results of which need to be joined by a set ``UNION
ALL``. It is therefore a relatively expensive operation. Also, it has
undefined behavior if the sets to be unioned are not disjoint (since
common elements may come from either subset, with potentially
different augmented values).

A simple example of the usage of the ``FOR`` clause is in conjunction
with ``INSERT``. It allows inserting objects in bulk.

.. code-block:: eql

    WITH MODULE example
    FOR x IN {
            (name := 'Alice', theme := 'fire'),
            (name := 'Bob', theme := 'rain'),
            (name := 'Carol', theme := 'clouds'),
            (name := 'Dave', theme := 'forest')
        }
    UNION OF (
        INSERT
            User {
                name := x.name,
                theme := x.theme,
            }
    );

The next logical example is a variation of a bulk ``UPDATE``. When
updating data that mostly or completely depends on the objects being
updated there's no need to use ``FOR`` clause and it is not advised to
use it for performance reasons.

.. code-block:: eql

    WITH MODULE example
    UPDATE User
    FILTER User.name IN {'Alice', 'Bob', 'Carol', 'Dave'}
    SET {
        theme := 'halloween'
    };

    # The above can be accomplished with a FOR clause,
    # but it is not recommended.
    WITH MODULE example
    FOR x IN {'Alice', 'Bob', 'Carol', 'Dave'}
    UNION OF (
        UPDATE User
        FILTER User.name = x
        SET {
            theme := 'halloween'
        }
    );

However, there are cases when a bulk update lots of external data,
that cannot be derived from the objects being updated. That is a good
use-case when a ``FOR`` clause is appropriate.

.. code-block:: eql

    # Here's an example of an update that is awkward to
    # express without the use of FOR clause
    WITH MODULE example
    UPDATE User
    FILTER User.name IN {'Alice', 'Bob', 'Carol', 'Dave'}
    SET {
        theme := 'red'  IF .name = 'Alice' ELSE
                 'star' IF .name = 'Bob' ELSE
                 'dark' IF .name = 'Carol' ELSE
                 'strawberry'
    };

    # Using a FOR clause, the above update becomes simpler to
    # express or review for a human.
    WITH MODULE example
    FOR x IN {
            (name := 'Alice', theme := 'red'),
            (name := 'Bob', theme := 'star'),
            (name := 'Carol', theme := 'dark'),
            (name := 'Dave', theme := 'strawberry')
        }
    UNION OF (
        UPDATE User
        FILTER User.name = x.name
        SET {
            theme := x.theme
        }
    );

Another example of using a ``FOR`` clause is working with link
properties. Specifying the link properties either at creation time or
in a later step with an update is often simpler with a ``FOR`` clause
helping to associate the link target to the link property in an
intuitive manner.

.. code-block:: eql

    # Expressing this without FOR clause is fairly tedious.
    WITH
        MODULE example,
        U2 := User
    FOR x IN {
            (
                name := 'Alice',
                friends := [('Bob', 'coffee buff'),
                            ('Carol', 'dog person')]
            ),
            (
                name := 'Bob',
                friends := [('Alice', 'movie buff'),
                            ('Dave', 'cat person')]
            )
        }
    UNION OF (
        UPDATE User
        FILTER User.name = x.name
        SET {
            friends := (
                FOR f in unnest(x.friends)
                UNION OF (
                    SELECT U2 {@nickname := f.1}
                    FILTER U2.name = f.0
                )
            )
        }
    );
