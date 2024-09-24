The inplace upgrade system adds three new flags to edgedb-server. They may (though probably usually won't) be specified together. If any of them is specified, the server will exit after performing the in-place upgrade operations instead of starting up.

 * ``-inplace-upgrade-prepare <file>`` -- "prepare" an inplace upgrade, using schema information provided in ``<file>``. (More about this later.) This will create the new standard library (in a namespace), populate the schema tables with user schemas, and prepare (but not execute) any irreversible scripts for updating the standard library trampolines and fixing up user-defined functions.

   This operation should not do anything that cannot be backed out.
   It may be run while an older version of the server is still live.

   If this is interrupted, crashes, or fails, it *will leave a partially prepared database*. To deal with this, see the next command.

   The file should be in the format produced by ``tests/inplace-testing/prep-upgrades.py``: a JSON object where the keys are branch names and the values are the results of executing ``administer prepare_upgrade()``.

 * ``-inplace-upgrade-rollback`` -- Rolls back a prepared upgrade.
   This works by deleting everything in the newly created schemas. It can rollback partially prepared upgrades.

   It may be run while an older version of the server is still live.

 * ``-inplace-upgrade-finalize`` -- Finalizes a prepared upgrade by fully flipping the database to the new version. This flips standard library trampolines, patches user-defined functions, and deletes the old standard library.

   The old version must not be running. (Though there is not a clear way to enforce this.)

   Finalize does a dry run of each branch's upgrade inside a transaction before making any changes. If this fails, the upgrade may be broken (due to a bug or an incompatibility), and it may still be rolled back.

   If finalize fails *after* the dry run, once it has started actually finalizing branches, then it *may not* be rolled back. Because all of the upgrades were tested in (reverted) transactions, this *should* only happen in the case of interruption or postgres crash, and it should be safe to retry the finalize.

   If finalize emitted a message of the form "Finished pivoting branch '<something>'", then the upgrade may not be rolled back; the only way out is through. Rollback will refuse to operate in this case.

-----

Suggested procedure:

0.5. Make a backup

1. ``edgedb query 'configure instance set force_database_error := $${"type": "AvailabilityError", "message": "DDL is disabled due to in-place upgrade.", "_scopes": ["ddl"]}$$;'``.
   This will disable all DDL commands to the database, while leaving it running for both read and write queries.

2. ``tests/inplace-testing/prep-upgrades.py > "upgrade.json"``.
   This will dump the information needed for upgrade.

3. ``edgedb-server --backend-dsn="$DSN" --inplace-upgrade-prepare upgrade.json``.
   This will prepare the upgrade.

4. Stop the old edgedb server.

4.5. Make a backup

5. ``edgedb-server --backend-dsn="$DSN" --inplace-upgrade-finalize``.
   This will finalize the upgrade.

6. Start the new server.

7. ``edgedb query 'configure instance reset force_database_error'``

If there is a failure in step 3 or step 5 *before* a branch has finished pivoting, then it can be rolled back with ``edgedb-server --backend-dsn="$DSN" --inplace-upgrade-rollback``.

If there is a failure after a branch has been pivoted, then there is nothing to do but retry it.
(And restore from a backup if that doesn't work. That would be a bug, and one that has slipped past at least one line of defence.)


----

Testing notes:

Currently, we can only inplace upgrade beween full major versions, since we use the major version number to distinguish between the namespaced stdlibs.

For testing inplace upgrades, we have a test that applies a patch that bumps the major version number and catalog.

TODO: Maybe we should use the catalog number instead, which will make it easier to test between different nightlies.
