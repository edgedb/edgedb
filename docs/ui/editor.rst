.. _ref_ui_editor:

======
Editor
======

.. image:: images/editor.png
    :alt: The editor page in the EdgeDB UI, inside which the query builder
          is shown as the user puts together a query to see the name property
          for a user-defined type called Book. A filter on the object's id
          and a limit to the number of object types returned are being set.
          The Editor icon is a blue square resembling a pad, with an orange
          line resembling a pencil on top.
    :width: 100%

Query Editor
------------

.. image:: images/editor.png
    :alt: The Editor tab in the EdgeDB UI editor page, showing a query
          appended with the analyze keyword to analyze performance. The
          performance results show up in a graph on the right, with separate
          colored rectangles for each link traversed by the query.
    :width: 100%

The query editor outwardly resembles the REPL with some auto-completion
functionality, but is most useful when paired with the ``analyze`` keyword.
Prepending ``analyze`` to the front of any query will display a visual
analyzer to help you understand the performance of your EdgeQL queries.

Query Builder
-------------

The query builder is by far the easiest way for new users to EdgeDB to
put together a ``select`` query, and the second easiest part of the UI
to use after the data explorer. The query builder is an entirely
point-and-click interface that walks you through the steps of a ``select``
query, including selecting properties and links inside an object,
``filter``, ``order by``, and setting an ``offset`` and ``limit``
(the maximum number of items to return from a query).

History
-------

The History button inside the Editor tab pulls up the most recently used
queries for both the query editor and query builder.