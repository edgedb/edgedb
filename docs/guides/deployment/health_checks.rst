.. _ref_guide_deployment_health_checks:

=============
Health Checks
=============

You may want to monitor the status of your EdgeDB instance. Is it up? Is it
ready to take queries? This guide will show you to perform health checks using
HTTP and the ``alive`` and ``ready`` endpoints.


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
