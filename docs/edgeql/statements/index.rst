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

Transaction control statements:

* :ref:`START TRANSACTION <ref_eql_statements_start_tx>`

  Start a transaction.

* :ref:`COMMIT <ref_eql_statements_commit_tx>`

  Commit the current transaction.

* :ref:`ROLLBACK <ref_eql_statements_rollback_tx>`

  Abort the current transaction.

* :ref:`DECLARE SAVEPOINT <ref_eql_statements_declare_savepoint>`

  Declare a savepoint within the current transaction.

* :ref:`ROLLBACK TO SAVEPOINT <ref_eql_statements_rollback_savepoint>`

  Rollback to a savepoint within the current transaction.

* :ref:`RELEASE SAVEPOINT <ref_eql_statements_release_savepoint>`

  Release a previously declared savepoint.

Session state control statements:

* :ref:`SET ALIAS <ref_eql_statements_session_set_alias>` and
  :ref:`RESET ALIAS <ref_eql_statements_session_reset_alias>`.

Introspection command:

* :ref:`DESCRIBE <ref_eql_statements_describe>`.


.. toctree::
    :maxdepth: 3
    :hidden:

    select
    for
    insert
    update
    delete
    with

    tx_start
    tx_commit
    tx_rollback
    tx_sp_declare
    tx_sp_release
    tx_sp_rollback

    sess_set_alias
    sess_reset_alias

    describe
