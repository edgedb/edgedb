.. _ref_edgeql_cheatsheet:


Syntax Cheat Sheet
==================

Link, link property and backwards link:

.. code-block:: eql

    SELECT Foo.bar;         # link
    SELECT Foo.bar@spam;    # link property
    SELECT Foo.<baz;        # backwards link
