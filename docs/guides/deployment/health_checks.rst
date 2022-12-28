.. _ref_guide_deployment_health_checks:

=============
Health Checks
=============

You may want to monitor the status of your EdgeDB instance. Is it up? Is it
ready to take queries? This guide will show you to perform health checks using
our HTTP client and the ``alive`` and ``ready`` endpoints.

Enable the HTTP Client
======================

Add this line to your schema, outside the ``module`` block:

.. code-block:: sdl

    using extension edgeql_http;

Then use the CLI to create a migration:

.. code-block:: bash

    $ edgedb migration create
    did you create extension 'edgeql_http'? [y,n,l,c,b,s,q,?]
    > y
    Created dbschema/migrations/00003.edgeql, id: <your-migration-id>

Your migration's filename and ID may be different.

Apply the new migration:

.. code-block:: bash

    $ edgedb migrate
    Applied <your-migration-id> (00003.edgeql)

EdgeDB will now expose HTTP endpoints you can use to interact with the
database.
    

Check Instance Aliveness
========================

To check if the instance is alive, make a request to this endpoint:

.. code-block::

    http://<hostname>:<port>/server/status/alive

To find your ``<port>``, you can run ``edgedb instance list`` to see a table of
all your instances along with their port numbers.

The endpoint will respond with a ``200`` status code and ``"OK"`` as the
payload if the server is alive. If not, you will receive a ``50x`` code or a
network error.

Check Instance Readiness
========================

To check if the instance is ready, make a request to this endpoint:

.. code-block::

    http://<hostname>:<port>/server/status/ready

As with the ``alive`` endpoint, you can find your ``<port>`` by running
``edgedb instance list`` to see a table of all your instances along with their
port numbers.

The endpoint will respond with a ``200`` status code and ``"OK"`` as the
payload if the server is ready. If not, you will receive a ``50x`` code or a
network error.
