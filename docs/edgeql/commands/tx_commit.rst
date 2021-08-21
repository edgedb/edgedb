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


.. _ref_eql_statements_commit_tx:

COMMIT
======

:eql-statement:


``COMMIT`` -- commit the current transaction

.. eql:synopsis::

    COMMIT ;


Example
-------

Commit the current transaction:

.. code-block:: edgeql

    COMMIT;


Description
-----------

``COMMIT`` commits the current transaction. All changes made by the
transaction become visible to others and are guaranteed to be durable
if a crash occurs.


See Also
--------

:ref:`START TRANSACTION <ref_eql_statements_start_tx>`,
:ref:`ROLLBACK <ref_eql_statements_rollback_tx>`,
:ref:`DECLARE SAVEPOINT <ref_eql_statements_declare_savepoint>`,
:ref:`ROLLBACK TO SAVEPOINT <ref_eql_statements_rollback_savepoint>`,
and :ref:`RELEASE SAVEPOINT <ref_eql_statements_release_savepoint>`.
