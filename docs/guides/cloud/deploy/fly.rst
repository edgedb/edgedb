.. _ref_guide_cloud_deploy_fly:

======
Fly.io
======

:edb-alt-title: Deploying applications built on EdgeDB Cloud to Fly.io

1. Install the `Fly.io CLI <https://fly.io/docs/hands-on/install-flyctl/>`_
2. Log in to Fly.io with ``flyctl auth login``
3. Run ``flyctl launch`` to create a new app on Fly.io and configure it.
   It will ask you to select a region and a name for your app. When done it will 
   create a ``fly.toml`` file and a ``Dockerfile`` in your project directory.
4. Set ``EDGEDB_INSTANCE`` and ``EDGEDB_SECRET_KEY`` as secrets in your Fly.io 
   app. 
   
   For **runtime secrets**, you can do this by running the following commands:

   .. code-block:: bash

    $ flyctl secrets set EDGEDB_INSTANCE <EDGEDB_INSTANCE>
    $ flyctl secrets set EDGEDB_SECRET_KEY <EDGEDB_SECRET_KEY>

   `Read more about Fly.io runtime secrets 
   <https://fly.io/docs/reference/secrets/>`_.

   For **build secrets**, you can do this by modifying the ``Dockerfile`` to 
   mount the secrets as environment variables.

   .. code-block:: dockerfile-diff
    :caption: Dockerfile
  
      # Build application
    -  RUN pnpm run build
    +  RUN --mount=type=secret,id=EDGEDB_INSTANCE \
    +      --mount=type=secret,id=EDGEDB_SECRET_KEY \
    +      EDGEDB_INSTANCE="$(cat /run/secrets/EDGEDB_INSTANCE)" \
    +      EDGEDB_SECRET_KEY="$(cat /run/secrets/EDGEDB_SECRET_KEY)" \
    +      pnpm run build

   `Read more about Fly.io build secrets 
   <https://fly.io/docs/reference/build-secrets/>`_.

5. Deploy your app to Fly.io

   .. code-block:: bash

    $ flyctl deploy

   If your app requires build secrets, you can pass them as arguments 
   to the ``deploy`` command:

   .. code-block:: bash

    $ flyctl deploy --build-secret EDGEDB_INSTANCE="<EDGEDB_INSTANCE>" \
        --build-secret EDGEDB_SECRET_KEY="<EDGEDB_SECRET_KEY>"
