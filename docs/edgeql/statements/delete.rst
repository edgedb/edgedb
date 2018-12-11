.. _ref_eql_statements_delete:

DELETE
======

:eql-statement:
:eql-haswith:

``DELETE`` -- remove objects from a database.

.. eql:synopsis::

    [ WITH <with-spec> [ , ... ] ]
    DELETE <expr> ;

:eql:synopsis:`WITH`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``UPDATE``
    statement.  See :ref:`ref_eql_with` for more information.

:eql:synopsis:`DELETE <expr>`
    Remove objects returned by *expr* from the database.


Output
~~~~~~

On successful completion, a ``DELETE`` statement returns the set
of deleted objects.


Examples
~~~~~~~~

Here's a simple example of deleting a specific user:

.. code-block:: edgeql

    WITH MODULE example
    DELETE (SELECT User
            FILTER User.name = 'Alice Smith');
