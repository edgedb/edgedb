.. _ref_guide_cloud_deploy_render:

======
Render
======

:edb-alt-title: Deploying applications built on |Gel| Cloud to Render

1. Push project to GitHub or some other Git remote repository
2. Create and make note of a secret key for your Gel Cloud instance
3. From Render's dashboard, click "New > Web Service"
4. Import your project's repository
5. In the setup page, scroll down to the "Environment Variables" section and
   add the following environment variables:

   - ``EDGEDB_INSTANCE`` containing your Gel Cloud instance name (in
     ``<org>/<instance-name>`` format)
   - ``EDGEDB_SECRET_KEY`` containing the secret key you created and noted
     previously.

6. Click Deploy

.. image:: images/cloud-render-config.png
    :width: 100%
    :alt: A screenshot of the Render deployment configuration view
          highlighting the environment variables section where a user
          will need to set the necessary variables for Gel Cloud instance
          connection.
