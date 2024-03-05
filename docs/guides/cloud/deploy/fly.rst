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
4. Open the ``Dockefile`` and add modify the ``build`` step to mount the EdgeDB 
   Cloud secret key and instance name as environment variables:

   .. code-block:: dockerfile-diff
    :caption: Dockerfile
  
      # Build application
    -  RUN pnpm run build
    +  RUN --mount=type=secret,id=EDGEDB_INSTANCE \
    +      --mount=type=secret,id=EDGEDB_SECRET_KEY \
    +      EDGEDB_INSTANCE="$(cat /run/secrets/EDGEDB_INSTANCE)" \
    +      EDGEDB_SECRET_KEY="$(cat /run/secrets/EDGEDB_SECRET_KEY)" \
    +      pnpm run build

5. Run ``flyctl deploy`` to deploy your app to Fly.io. You need to pass the 
   secrets to the deployment command:

   .. code-block:: bash

    $ flyctl deploy --build-secret EDGEDB_INSTANCE="<EDGEDB_INSTANCE>" \
        --build-secret EDGEDB_SECRET_KEY="<EDGEDB_SECRET_KEY>"
