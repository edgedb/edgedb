.. _ref_guide_cloud:

=====
Cloud
=====

:edb-alt-title: Using Gel Cloud

.. toctree::
    :maxdepth: 2
    :hidden:

    cli
    web
    http_gql
    deploy/index
    deploy/vercel
    deploy/netlify
    deploy/fly
    deploy/render
    deploy/railway


|Gel| Cloud is a fully managed, effortless cloud database service,
engineered to let you deploy your database instantly and connect from
anywhere with near-zero configuration.

Connecting your app
===================

Try a guide for connecting your app running on your platform of choice:

.. TODO: render these with icons

* :ref:`Vercel <ref_guide_cloud_deploy_vercel>`
* :ref:`Netlify <ref_guide_cloud_deploy_netlify>`
* :ref:`Fly.io <ref_guide_cloud_deploy_fly>`
* :ref:`Render <ref_guide_cloud_deploy_render>`
* :ref:`Railway <ref_guide_cloud_deploy_railway>`

To connect your apps running on other platforms, generate a dedicated
secret key for your instance with :gelcmd:`cloud secretkey create` or via the
web UI's “Secret Keys” pane in your instance dashboard. Create two environment
variables accessible to your production application:

* :gelenv:`SECRET_KEY` - contains the secret key you generated
* :gelenv:`INSTANCE` - the name of your |Gel| Cloud instance (``<org-name>/<instance-name>``)


Two ways to use Gel Cloud
=========================

1. CLI
^^^^^^

Log in to |Gel| Cloud via the CLI:

.. code-block:: bash

  $ gel cloud login


This will open a browser window and allow you to log in via GitHub.
Now, create your |Gel| Cloud instance the same way you would create a
local instance:

.. code-block:: bash

  $ gel instance create <org-name>/<instance-name>

or

.. code-block:: bash

  $ gel project init \
  --server-instance <org-name>/<instance-name>


2. GUI
^^^^^^

Create your instance at `cloud.geldata.com <https://cloud.geldata.com>`_ by
clicking on “Create new instance” in the “Instances” tab.

.. <div className={styles.cloudGuiImg} />

Complete the following form to configure your instance. You can access
your instance via the CLI using the name ``<org-name>/<instance-name>`` or via the GUI.


Useful Gel Cloud commands
=========================

Get REPL
^^^^^^^^

.. code-block:: bash

  $ gel \
    -I <org-name>/<instance-name>

Run migrations
^^^^^^^^^^^^^^

.. code-block:: bash

  $ gel migrate \
    -I <org-name>/<instance-name>

Update your instance
^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

  $ gel instance upgrade \
    --to-version <target-version> \
    -I <org-name>/<instance-name>

Manual full backup
^^^^^^^^^^^^^^^^^^

.. code-block:: bash

  $ gel dump \
    --all --format dir \
    -I <org-name>/<instance-name> \
    <local-dump-path>

Full restore
^^^^^^^^^^^^

.. code-block:: bash

  $ gel restore \
    --all \
    -I <org-name>/<instance-name> \
    <local-dump-path>

.. note::

  Restoring works only to an empty database.


Questions? Problems? Bugs?
==========================

Thank you for helping us make the best way to host your |Gel| instances even
better!

* Please join us on `our Discord <https://discord.gg/umUueND6ag>`_  to ask
  questions.
* If you're experiencing a service interruption, check `our status page
  <https://www.gelstatus.com/>`_ for information on what may be
  causing it.
* Report any bugs you find by `submitting a support ticket
  <https://www.geldata.com/p/cloud-support>`_. Note: when using |Gel| Cloud
  through the CLI, setting the ``RUST_LOG`` environment variable to ``info``,
  ``debug``, or ``trace`` may provide additional debugging information
  which will be useful to include with your ticket.
