..
    Portions Copyright (c) 2019 MagicStack Inc. and the EdgeDB authors.

    Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
    Portions Copyright (c) 1994, The Regents of the University of California

    Permission to use, copy, modify, and distribute this software and its
    documentation for any purpose, without fee, and without a written agreement
    is hereby granted, provided that the above copyright notice and this
    paragraph and the following two paragraphs appear in all copies.

    IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
    DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
    LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
    DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.

    THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
    INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
    AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
    ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
    PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.


.. _ref_eql_statements_start_tx:

Start transaction
=================

:eql-statement:


``start transaction`` -- start a transaction

.. eql:synopsis::

    start transaction <transaction-mode> [ , ... ] ;

    # where <transaction-mode> is one of:

    isolation serializable
    read write | read only
    deferrable | not deferrable


Description
-----------

This command starts a new transaction block.

Any EdgeDB command outside of an explicit transaction block starts
an implicit transaction block; the transaction is then automatically
committed if the command was executed successfully, or automatically
rollbacked if there was an error.  This behavior is often called
"autocommit".


Parameters
----------

The :eql:synopsis:`<transaction-mode>` can be one of the following:

:eql:synopsis:`isolation serializable`
    All statements in the current transaction can only see data 
    changes that were committed before the first query or data 
    modification statement was executed within this transaction. 
    If a pattern of reads and writes among concurrent serializable
    transactions creates a situation that could not have occurred 
    in any serial (one-at-a-time) execution of those transactions, 
    one of them will be rolled back with a serialization_failure error.

:eql:synopsis:`read write`
    Sets the transaction access mode to read/write.

    This is the default.

:eql:synopsis:`read only`
    Sets the transaction access mode to read-only.  Any data
    modifications with :eql:stmt:`insert`, :eql:stmt:`update`, or
    :eql:stmt:`delete` are disallowed. Schema mutations via :ref:`DDL
    <ref_eql_ddl>` are also disallowed.

:eql:synopsis:`deferrable`
    The transaction can be set to deferrable mode only when it is
    ``serializable`` and ``read only``.  When all three of these
    properties are selected for a transaction, the transaction
    may block when first acquiring its snapshot, after which it is
    able to run without the normal overhead of a ``serializable``
    transaction and without any risk of contributing to or being
    canceled by a serialization failure. This mode is well suited
    for long-running reports or backups.


Examples
--------

Start a new transaction and rollback it:

.. code-block:: edgeql

    start transaction;
    select 'Hello World!';
    rollback;

Start a serializable deferrable transaction:

.. code-block:: edgeql

    start transaction isolation serializable, read only, deferrable;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > Commit
      <ref_eql_statements_commit_tx>`
  * - :ref:`Reference > EdgeQL > Rollback
      <ref_eql_statements_rollback_tx>`
  * - :ref:`Reference > EdgeQL > Declare savepoint
      <ref_eql_statements_declare_savepoint>`
  * - :ref:`Reference > EdgeQL > Rollback to savepoint
      <ref_eql_statements_rollback_savepoint>`
  * - :ref:`Reference > EdgeQL > Release savepoint
      <ref_eql_statements_release_savepoint>`
