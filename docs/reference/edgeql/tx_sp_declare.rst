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


.. _ref_eql_statements_declare_savepoint:

Declare savepoint
=================

:eql-statement:


``declare savepoint`` -- declare a savepoint within the current transaction

.. eql:synopsis::

    declare savepoint <savepoint-name> ;


Description
-----------

``savepoint`` establishes a new savepoint within the current
transaction.

A savepoint is a special mark inside a transaction that allows all
commands that are executed after it was established to be rolled back,
restoring the transaction state to what it was at the time of the
savepoint.

It is an error to declare a savepoint outside of a transaction.


Example
-------

.. code-block:: edgeql

    # Will select no objects:
    select test::TestObject { name };

    start transaction;

        insert test::TestObject { name := 'q1' };
        insert test::TestObject { name := 'q2' };

        # Will select two TestObjects with names 'q1' and 'q2'
        select test::TestObject { name };

        declare savepoint f1;
            insert test::TestObject { name:='w1' };

            # Will select three TestObjects with names
            # 'q1' 'q2', and 'w1'
            select test::TestObject { name };
        rollback to savepoint f1;

        # Will select two TestObjects with names 'q1' and 'q2'
        select test::TestObject { name };

    rollback;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > Start transaction
      <ref_eql_statements_start_tx>`
  * - :ref:`Reference > EdgeQL > Commit
      <ref_eql_statements_commit_tx>`
  * - :ref:`Reference > EdgeQL > Rollabck
      <ref_eql_statements_rollback_tx>`
  * - :ref:`Reference > EdgeQL > Rollback to savepoint
      <ref_eql_statements_rollback_savepoint>`
  * - :ref:`Reference > EdgeQL > Release savepoint
      <ref_eql_statements_release_savepoint>`
