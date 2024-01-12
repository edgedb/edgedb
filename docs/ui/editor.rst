.. _ref_ui_editor:

======
Editor
======

.. image:: images/editor.png
    :alt: The Editor page in the EdgeDB UI, inside which the Query Builder
          is shown as the user puts together a query to see the name property
          for a user-defined type called Book. A filter on the object's id
          and a limit to the number of object types returned are being set.
          The Editor icon is a blue square resembling a pad, with an orange
          line resembling a pencil on top.
    :width: 100%

Query Editor
------------

The Query Editor is outwardly similar to the REPL besides some
auto-completion, but is most useful when paired with the ``analyze``
keyword. Prepending ``analyze`` to the front of any query will
display a visual query analyzer to help you tweak performance on your
EdgeQL queries.

Query Builder
-------------

The Query Builder is by far the easiest way for new users to EdgeDB to
put together a ``select`` query, and the second easiest part of the UI
to use after the Data Explorer. The Query Builder is an entirely
point-and-click interface that walks you through the steps of a ``select``
query, including selecting properties and links inside an object,
``filter``, ``order by``, and setting an ``offset`` and ``limit``
(the maximum number of items to return from a query).

History
-------

The History button inside the Editor tab pulls up the most recently used
queries for both the Query Editor and Query Builder.