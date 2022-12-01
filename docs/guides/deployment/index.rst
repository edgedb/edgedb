.. _ref_guide_deployment:


==========
Deployment
==========

EdgeDB can be hosted on all major cloud hosting platforms. The guides below
demonstrate how to spin up both a managed PostgreSQL instance and a container
running EdgeDB `in Docker <https://github.com/edgedb/edgedb-docker>`_.

.. note:: Minimum requirements

    As a rule of thumb, the EdgeDB Docker container requires 1GB RAM! Images
    with insufficient RAM may experience unexpected issues during startup.

.. toctree::
    :maxdepth: 1

    aws_aurora_ecs
    azure_flexibleserver
    digitalocean
    fly_io
    gcp
    heroku
    docker
    bare_metal
