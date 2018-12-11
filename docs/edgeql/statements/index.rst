.. _ref_eql_statements:

Statements
==========

Statements in EdgeQL are a kind of an *expression* that has one or
more ``clauses`` and is used to retrieve or modify data in a database.

Query statements:

* :ref:`SELECT <ref_eql_statements_select>`

  Retrieve data from a database and compute arbitrary expressions.

* :ref:`FOR <ref_eql_statements_for>`

  Compute an expression for every element of an input set and
  concatenate the results.

Data modification statements:

* :ref:`INSERT <ref_eql_statements_insert>`

  Create new object in a database.

* :ref:`UPDATE <ref_eql_statements_update>`

  Update objects in a database.

* :ref:`DELETE <ref_eql_statements_delete>`

  Remove objects from a database.


.. toctree::
    :maxdepth: 3
    :hidden:

    select
    for
    insert
    update
    delete
    with
