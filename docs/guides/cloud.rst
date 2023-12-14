.. _ref_guide_cloud:

=====
Cloud
=====

:edb-alt-title: Using EdgeDB Cloud

EdgeDB Cloud is the easiest way to host your EdgeDB instance. We offer two ways
to interact with EdgeDB Cloud: via our CLI or through a graphical web
interface nearly identical to the :ref:`EdgeDB UI <ref_cli_edgedb_ui>`.

.. edb:youtube-embed:: IG1MggUzzH4


CLI Cloud Interface
===================

To use Cloud via the CLI, first log in using :ref:`ref_cli_edgedb_cloud_login`.
Once your login is successful, you will be able to create an instance using
either :ref:`ref_cli_edgedb_instance_create` or
:ref:`ref_cli_edgedb_project_init`, depending on whether you also want to
create a local project linked to your instance.

* :ref:`ref_cli_edgedb_instance_create` with an instance name of
  ``<org-name>/<instance-name>``.

  .. code-block:: bash

      $ edgedb instance create <org-name>/<instance-name>

* :ref:`ref_cli_edgedb_project_init` with the ``--server-instance`` option. Set
  the server instance name to ``<org-name>/<instance-name>``.

  .. code-block:: bash

      $ edgedb project init \
        --server-instance <org-name>/<instance-name>

  Alternatively, you can run ``edgedb project init`` *without* the
  ``--server-instance`` option and enter an instance name in the
  ``<org-name>/<instance-name>`` format when prompted interactively.

.. note::

    Please be aware of the following restrictions on EdgeDB Cloud instance
    names:

    * can contain only Latin alpha-numeric characters or ``-``
    * cannot start with a dash (``-``) or contain double dashes (``--``)
    * maximum instance name length is 61 characters minus the length of your
      organization name (i.e., length of organization name + length of instance
      name must be fewer than 62 characters)

To use ``edgedb instance create``:

.. code-block:: bash

    $ edgedb instance create <org-name>/<instance-name>

To use ``edgedb project init``:

.. code-block:: bash

    $ edgedb project init \
      --server-instance <org-name>/<instance-name>

Alternatively, you can run ``edgedb project init`` *without* the
``--server-instance`` option and enter an instance name in the
``<org-name>/<instance-name>`` format when prompted interactively.


Web Cloud Interface
===================

If you'd prefer, you can also manage your account via `the EdgeDB Cloud
web-based GUI <https://cloud.edgedb.com/>`_.

The first time you access the web UI, you will be prompted to log in. Once you
log in with your account, you'll be on the "Instances" tab of the front page
which shows your instance list. The other two tabs allow you to manage your
organization settings and billing.

Instances
---------

If this is your first time accessing EdgeDB Cloud, this list will be empty. To
create an instance, click "Create new instance." This will pop up a modal
allowing you to name your instance and specify the version of EdgeDB and the
region for the instance.

Once the instance has been created, you'll see the instance dashboard which
allows you to monitor your instance, navigate to the management page for its
databases, and create secret keys.

You'll also see instructions in the bottom-right for linking your EdgeDB CLI to
your EdgeDB Cloud account. You do this by running the CLI command ``edgedb
cloud login``. This will make all of your EdgeDB Cloud instances accessible via
the CLI. You can manage them just as you would other remote EdgeDB instances.

If you want to manage a database inside your instance, click through on 
its name from the top right of the instance dashboard. If you just created
a database, the database management view will be mostly empty except for a
button offering to create a sample database. Once you have a schema created
and some data in a database, this view will offer you similar tools to those
in our local UI.

You'll be able to access a REPL, edit complex queries or build them
graphically, inspect your schema, and browse your data.

Org Settings
------------

This tab allows you to add GitHub organizations for which you are an admin. 
If you don't see your organization's name here, you may need to update your
`org settings`_ in GitHub to allow EdgeDB Cloud to read your list of 
organizations, and then refresh the org list.

.. lint-off

.. _org setings:
  https://docs.github.com/en/organizations/managing-oauth-access-to-your-organizations-data/approving-oauth-apps-for-your-organization

.. lint-on

Billing
-------

On this page you can manage your account type and payment methods, and set your
email for receiving billing info. Optionally, you can also save your payment
info using `Link <https://link.com/>`_, `Stripe's <https://stripe.com/>`_
fast-checkout solution.

Deploying your application
==========================

For your production deployment, generate a dedicated secret key for your
instance with :ref:`ref_cli_edgedb_cloud_secretkey_create` or via the web UI's
"Secret Keys" pane in your instance dashboard. Create two environment variables
accessible to your production application:

* ``EDGEDB_SECRET_KEY``- contains the secret key you generated
* ``EDGEDB_INSTANCE``- the name of your EdgeDB Cloud instance
  (``<org-name>/<instance-name>``)


Questions? Problems? Bugs?
==========================

Thank you for helping us make the best way to host your EdgeDB instances even
better!

* Please join us on `our Discord <https://discord.gg/umUueND6ag>`_  to ask
  questions.
* If you're experiencing a service interruption, check `our status page
  <https://www.edgedbstatus.com/>`_ for information on what may be
  causing it.
* Report any bugs you find by `submitting a support ticket
  <https://edgedb.com/p/cloud-support>`_. Note: when using EdgeDB Cloud
  through the CLI, setting the ``RUST_LOG`` environment variable to ``info``,
  ``debug``, or ``trace`` may provide additional debugging information
  which will be useful to include with your ticket.
