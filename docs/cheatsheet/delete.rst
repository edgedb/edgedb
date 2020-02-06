.. _ref_cheatsheet_delete:

Delete
======

.. note::

    The types used in these queries are defined :ref:`here
    <ref_cheatsheet_types>`.

Delete all reviews from a specific user:

.. code-block:: edgeql

    DELETE Review
    FILTER .author.name = 'trouble2020'

Alternative way to delete all reviews from a specific user:

.. code-block:: edgeql

    DELETE (
        SELECT User
        FILTER .name = 'troll2020'
    ).<author[IS Review]
