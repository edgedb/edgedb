.. _ref_guide_cloud_deploy:

=============
Deploy an app
=============

:edb-alt-title: Deploying applications built on EdgeDB Cloud

For your production deployment, generate a dedicated secret key for your
instance with :ref:`ref_cli_edgedb_cloud_secretkey_create` or via the web UI's
"Secret Keys" pane in your instance dashboard. Create two environment variables
accessible to your production application:

* ``EDGEDB_SECRET_KEY``- contains the secret key you generated
* ``EDGEDB_INSTANCE``- the name of your EdgeDB Cloud instance
  (``<org-name>/<instance-name>``)

If you use one of these platforms, try the platform's guide for
platform-specific instructions:

.. toctree::
    :maxdepth: 1

    vercel
    netlify
    fly
    railway
    render
