.. _ref_eql_statements:

EdgeQL
========

Statements in EdgeQL are a kind of an *expression* that has one or
more ``clauses`` and is used to retrieve or modify data in a database.

Query statements:

* :eql:stmt:`select`

  Retrieve data from a database and compute arbitrary expressions.

* :eql:stmt:`for`

  Compute an expression for every element of an input set and
  concatenate the results.

* :eql:stmt:`group`

  Group data into subsets by keys.

Data modification statements:

* :eql:stmt:`insert`

  Create new object in a database.

* :eql:stmt:`update`

  Update objects in a database.

* :eql:stmt:`delete`

  Remove objects from a database.

Transaction control statements:

* :eql:stmt:`start transaction`

  Start a transaction.

* :eql:stmt:`commit`

  Commit the current transaction.

* :eql:stmt:`rollback`

  Abort the current transaction.

* :eql:stmt:`declare savepoint`

  Declare a savepoint within the current transaction.

* :eql:stmt:`rollback to savepoint`

  Rollback to a savepoint within the current transaction.

* :eql:stmt:`release savepoint`

  Release a previously declared savepoint.

Session state control statements:

* :eql:stmt:`set` and :eql:stmt:`reset`.

Introspection command:

* :eql:stmt:`describe`.

.. versionadded:: 3.0

    Performance analysis statement:

    * :eql:stmt:`analyze`.


.. toctree::
    :maxdepth: 3
    :hidden:


    lexical
    eval
    shapes
    paths
    casts
    functions
    cardinality
    volatility

    select
    insert
    update
    delete
    for
    group
    with
    analyze

    tx_start
    tx_commit
    tx_rollback
    tx_sp_declare
    tx_sp_release
    tx_sp_rollback

    sess_set_alias
    sess_reset_alias

    describe
