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


.. _ref_eql_statements_release_savepoint:

RELEASE SAVEPOINT
=================

:eql-statement:


``RELEASE SAVEPOINT`` -- release a previously declared savepoint

.. eql:synopsis::

    RELEASE SAVEPOINT <savepoint-name> ;


Description
-----------

``RELEASE SAVEPOINT`` destroys a savepoint previously defined in the
current transaction.

Destroying a savepoint makes it unavailable as a rollback point,
but it has no other user visible behavior. It does not undo the effects
of commands executed after the savepoint was established.
(To do that, see
:ref:`ROLLBACK TO SAVEPOINT <ref_eql_statements_rollback_savepoint>`.)

``RELEASE SAVEPOINT`` also destroys all savepoints that were
established after the named savepoint was established.


Example
-------

.. code-block:: edgeql

    START TRANSACTION;
    # ...
    DECLARE SAVEPOINT f1;
    # ...
    RELEASE SAVEPOINT f1;
    # ...
    ROLLBACK;


See Also
--------

:ref:`START TRANSACTION <ref_eql_statements_start_tx>`,
:ref:`COMMIT <ref_eql_statements_commit_tx>`,
:ref:`ROLLBACK <ref_eql_statements_rollback_tx>`,
:ref:`DECLARE SAVEPOINT <ref_eql_statements_declare_savepoint>`, and
:ref:`ROLLBACK TO SAVEPOINT <ref_eql_statements_rollback_savepoint>`.
