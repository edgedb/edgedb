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

START TRANSACTION
=================

:eql-statement:


``START TRANSACTION`` -- start a transaction

.. eql:synopsis::

    START TRANSACTION <transaction-mode> [ , ... ] ;

    # where <transaction-mode> is one of:

    ISOLATION { SERIALIZABLE | REPEATABLE READ }
    READ WRITE | READ ONLY
    DEFERRABLE | NOT DEFERRABLE


Description
-----------

This command starts a new transaction block.

Any EdgeDB command outside of an explicit transaction block starts
an implicit transaction block; the transaction is then automatically
committed if the command was executed successfully, or automatically
rollbacked if there was an error.  This behaviour is often called
"autocommit".


Parameters
----------

The :eql:synopsis:`<transaction-mode>` can be one of the following:

:eql:synopsis:`ISOLATION SERIALIZABLE`
    All statements of the current transaction can only see data
    changes committed before the first query or data-modification
    statement was executed in this transaction.  If a pattern
    of reads and writes among concurrent serializable
    transactions would create a situation which could not have
    occurred for any serial (one-at-a-time) execution of those
    transactions, one of them will be rolled back with a
    serialization_failure error.

:eql:synopsis:`ISOLATION REPEATABLE READ`
    All statements of the current transaction can only see data
    committed before the first query or data-modification statement
    was executed in this transaction.

    This is the default.

:eql:synopsis:`READ WRITE`
    Sets the transaction access mode to read/write.

    This is the default.

:eql:synopsis:`READ ONLY`
    Sets the transaction access mode to read-only.  Any data
    modifications with :ref:`INSERT <ref_eql_statements_insert>`,
    :ref:`UPDATE <ref_eql_statements_update>`, or
    :ref:`DELETE <ref_eql_statements_delete>` are disallowed.
    Schema mutations via :ref:`DDL <ref_eql_ddl>` are also
    disallowed.

:eql:synopsis:`DEFERRABLE`
    The transaction can be set to deferrable mode only when it is
    ``SERIALIZABLE`` and ``READ ONLY``.  When all three of these
    properties are selected for a transaction, the transaction
    may block when first acquiring its snapshot, after which it is
    able to run without the normal overhead of a ``SERIALIZABLE``
    transaction and without any risk of contributing to or being
    canceled by a serialization failure. This mode is well suited
    for long-running reports or backups.


Examples
--------

Start a new transaction and rollback it:

.. code-block:: edgeql

    START TRANSACTION;
    SELECT 'Hello World!';
    ROLLBACK;

Start a serializable deferrable transaction:

.. code-block:: edgeql

    START TRANSACTION ISOLATION SERIALIZABLE, READ ONLY, DEFERRABLE;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > COMMIT
      <ref_eql_statements_commit_tx>`
  * - :ref:`Reference > EdgeQL > ROLLBACK
      <ref_eql_statements_rollback_tx>`,
  * - :ref:`Reference > EdgeQL > DECLARE SAVEPOINT
      <ref_eql_statements_declare_savepoint>`
  * - :ref:`Reference > EdgeQL > ROLLBACK TO SAVEPOINT
      <ref_eql_statements_rollback_savepoint>`
  * - :ref:`Reference > EdgeQL > RELEASE SAVEPOINT
      <ref_eql_statements_release_savepoint>`
