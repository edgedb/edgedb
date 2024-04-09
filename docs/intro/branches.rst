.. _ref_intro_branches:

========
Branches
========

EdgeDB's branches make it easy to prototype app features that impact your
database schema, even in cases where those features are never released. You can
create a branch in your EdgeDB database that corresponds to a feature branch in
your VCS. When you're done, either :ref:`merge <ref_cli_edgedb_branch_merge>`
that branch into your main branch or :ref:`drop <ref_cli_edgedb_branch_drop>`
it leaving your original schema intact.

.. note::

    The procedure we will describe should be adaptable to any VCS offering
    branching and rebasing, but in order to make the examples concrete and
    easy-to-follow, we'll be demonstrating how EdgeDB branches interact with
    Git branches. You may adapt these examples to your VCS of choice.


1. Create a new feature branch
------------------------------

Create a feature branch in your VCS and switch to it. Then, create and switch
to a corresponding branch in EdgeDB using the CLI.

.. code-block:: bash

    $ edgedb branch create feature
    Creating branch 'feature'...
    OK: CREATE BRANCH
    $ edgedb branch switch feature
    Switching from 'main' to 'feature'

.. note::

    You can alternatively create and switch in one shot using ``edgedb branch
    switch -c feature``.


2. Build your feature
---------------------

Write your code and make any schema changes your feature requires.


3. Pull any changes on ``main``
-------------------------------

.. note::

    This step is optional. If you know your ``main`` code branch is current and
    all migrations in that code branch have already been applied to your
    ``main`` database branch, feel free to skip it.

We need to make sure that merging our feature branch onto ``main`` is a simple
fast-forward. The next two steps take care of that.

Switch back to your ``main`` code branch. Run ``git pull`` to pull down any new
changes. If any of these are schema changes, use ``edgedb branch switch main``
to switch back to your ``main`` database branch and apply the new schema with
``edgedb migrate``.

Once this is done, you can switch back to your feature branches in your VCS and
EdgeDB.


4. Rebase your feature branch on ``main``
-----------------------------------------

.. note::

    If you skipped the previous step, you can skip this one too. This is only
    necessary if you had to pull down new changes on ``main``.

For your code branch, first make sure you're on ``feature`` and then run the
rebase:

.. code-block:: bash

    $ git rebase main

Now, do the same for your database, also from ``feature``:

.. code-block:: bash

    $ edgedb branch rebase main


5. Merge ``feature`` onto ``main``
----------------------------------

Switch back to both ``main`` branches and merge ``feature``.

.. code-block:: bash

    $ git switch main
    <changes>
    Switched to branch 'main'
    $ git merge feature

.. code-block:: bash

    $ edgedb branch switch main
    Switching from 'feature' to 'main'
    $ edgedb branch merge feature

Now, your feature and its schema have been successfully merged! 🎉


Further reading
^^^^^^^^^^^^^^^

- :ref:`Branches CLI <ref_cli_edgedb_branch>`

Further information can be found in the `branches RFC
<https://github.com/edgedb/rfcs/blob/master/text/1025-branches.rst#rebasing-branches>`_,
which describes the design of the migration system.
