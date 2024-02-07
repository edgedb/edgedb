.. _ref_ui_data_explorer:

=============
Data Explorer
=============

.. image:: images/data_explorer.png
    :alt: The data explorer page in the EdgeDB UI. The icon is three bars
          stacked on top of each other: blue, purple, and orange. A sample
          query via the Data Explorer shows information about a user-defined
          object type called a Booking.
    :width: 100%

The data explorer is similar to the UI editor in facilitating queries on
database objects, but involves no direct query building (aside from filters,
which by nature must be specified manually). Instead, the data explorer gives
point-and-click access to the database's objects, including inserting
new objects and modifying existing ones.

This makes the data explorer the ideal solution for EdgeDB users without
a technical background or new users lacking the time needed to learn the
ins and outs of a new database. It is also useful for skilled users
who are looking to explore the links between object types over possible
multiple levels of depth without having to continually write new queries
to do so.