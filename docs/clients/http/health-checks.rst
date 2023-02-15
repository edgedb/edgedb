.. _ref_edgeql_http_health_checks:

=============
Health Checks
=============

EdgeDB exposes HTTP endpoints to check for aliveness and readiness of your
database instance. You can make GET requests to these endpoints to check the
instance status.

Aliveness
---------

Check that your instance is alive by making a request to
``http://<hostname>:<port>/server/status/alive``. If your instance is alive, it
will respond with a ``200`` status code and ``"OK"`` as the payload. Otherwise,
it will respond with a ``50x`` or a network error.

Readiness
---------

Check that your instance is ready by making a request to
``http://<hostname>:<port>/server/status/ready``. If your instance is ready, it
will respond with a ``200`` status code and ``"OK"`` as the payload. Otherwise,
it will respond with a ``50x`` or a network error.
