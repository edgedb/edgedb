.. _ref_guide_cloud_deploy_fly:

======
Fly.io
======

:edb-alt-title: Deploying applications built on Gel Cloud to Fly.io

1. Install the `Fly.io CLI <https://fly.io/docs/hands-on/install-flyctl/>`_
2. Log in to Fly.io with ``flyctl auth login``
3. Run ``flyctl launch`` to create a new app on Fly.io and configure it.
   It will ask you to select a region and a name for your app. When done it will
   create a ``fly.toml`` file and a ``Dockerfile`` in your project directory.
4. Set :gelenv:`INSTANCE` and :gelenv:`SECRET_KEY` as secrets in your Fly.io
   app.

   For **runtime secrets**, you can do this by running the following commands:

   .. code-block:: bash

    $ flyctl secrets set GEL_INSTANCE <GEL_INSTANCE>
    $ flyctl secrets set GEL_SECRET_KEY <GEL_SECRET_KEY>

   `Read more about Fly.io runtime secrets
   <https://fly.io/docs/reference/secrets/>`_.

   For **build secrets**, you can do this by modifying the ``Dockerfile`` to
   mount the secrets as environment variables.

   .. code-block:: dockerfile-diff
    :caption: Dockerfile

      # Build application
    -  RUN pnpm run build
    +  RUN --mount=type=secret,id=GEL_INSTANCE \
    +      --mount=type=secret,id=GEL_SECRET_KEY \
    +      GEL_INSTANCE="$(cat /run/secrets/GEL_INSTANCE)" \
    +      GEL_SECRET_KEY="$(cat /run/secrets/GEL_SECRET_KEY)" \
    +      pnpm run build

   `Read more about Fly.io build secrets
   <https://fly.io/docs/reference/build-secrets/>`_.

5. Deploy your app to Fly.io

   .. code-block:: bash

    $ flyctl deploy

   If your app requires build secrets, you can pass them as arguments
   to the ``deploy`` command:

   .. code-block:: bash

    $ flyctl deploy --build-secret GEL_INSTANCE="<GEL_INSTANCE>" \
        --build-secret GEL_SECRET_KEY="<GEL_SECRET_KEY>"
