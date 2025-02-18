.. _ref_guide_cloud_deploy_railway:

=======
Railway
=======

:edb-alt-title: Deploying applications built on |Gel| Cloud to Railway

1. Push project to GitHub or some other Git remote repository
2. Create and make note of a secret key for your Gel Cloud instance
3. From Railway's dashboard, click the "New Project" button
4. Select the repository you want to deploy
5. Click the "Add variables" button to add the following environment variables:

   - :gelenv:`INSTANCE` containing your Gel Cloud instance name (in
     ``<org>/<instance-name>`` format)
   - :gelenv:`SECRET_KEY` containing the secret key you created and noted
     previously.

6. Click "Deploy"

.. image:: images/cloud-railway-config.png
    :width: 100%
    :alt: A screenshot of the Railway deployment configuration view
          highlighting the environment variables section where a user will
          need to set the necessary variables for Gel Cloud instance
          connection.
