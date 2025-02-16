.. _ref_ui_schema_viewer:

=============
Schema viewer
=============

The UI schema viewer allows you to see your schema either as text, graph,
or both.

Text view
---------

The UI's text view is similar to what is produced in the CLI through the
``describe schema as sdl`` command, but includes inherited properties and
links.

The schema viewer shows the user's defined schema by default under the name
``User``. The user may instead select and view ``Stdlib`` or ``System`` by
selecting them from the Schema dropdown menu.

.. image:: images/schema_selection_dropdown.png
    :alt: The schema selection dropdown menu highlighted in the context of the
          overall schema viewer, showing the menu open. Three schemas are
          available for selection: User, Stdlib, and System, with User
          currently active (indicated by a check mark) and Stdlib currently
          selected (highlighted in green).
    :width: 100%

Along with the schema viewer's built-in search function, this makes
the schema viewer often even more convenient than searching through EdgeDB's
online documentation.

For example, a search for 'max val' immediately shows the following output:

.. image:: images/stdlib_search.png
    :alt: A search result from the Schema Viewer page in the EdgeDB UI,
          in which a search for the keywords max and val has turned up
          three matching functions: max value, max ex value, and max
          len value. The full code for each function as declared can be
          seen, allowing for inspection of their behavior.
    :width: 100%

Graph view
----------

.. image:: images/schema_viewer.png
    :alt: The Schema Viewer page in the EdgeDB UI. The icon is two small
          squares of blue and orange, connected by a purple line. A small
          user-defined sample schema is shown with two concrete types
          called Book and Library, along with an abstract type called
          HasAddress that is extended by the Library type.
    :width: 100%

- Object types are shown as either blue boxes for concrete types, or gray
  boxes for abstract types. Object types that extend another object type
  will show this via a gray arrow.
- Links show up as purple arrows from one type to another, with any link
  properties shown tucked inside the arrow.
- Zooming in and out along with dragging and dropping boxes allow you to
  visually interact with your schema.