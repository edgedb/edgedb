.. _ref_guide_deployment_gcp:

============
Google Cloud
============

:edb-alt-title: Deploying EdgeDB to Google Cloud

In this guide we show how to deploy EdgeDB on GCP using Cloud SQL and
Kubernetes.

Prerequisites
=============

* Google Cloud account with billing enabled (or a `free trial <gcp-trial_>`_)
* ``gcloud`` CLI (`install <gcloud-intsll_>`_)
* ``kubectl`` CLI (`install <kubectl-install_>`_)

.. _gcp-trial: https://cloud.google.com/free/
.. _gcloud-intsll: https://cloud.google.com/sdk/
.. _kubectl-install: https://kubernetes.io/docs/tasks/tools/install-kubectl/

Make sure you are logged into Google Cloud.

.. code-block:: bash

   $ gcloud init

Create a project
================

Set the ``PROJECT`` environment variable to the project name you'd like to
use. Google Cloud only allow letters, numbers, and hyphens.

.. code-block:: bash

   $ PROJECT=edgedb

Then create a project with this name. Skip this step if your project already
exists.

.. code-block:: bash

   $ gcloud projects create $PROJECT

Then enable the requisite APIs.

.. code-block:: bash

   $ gcloud services enable \
       container.googleapis.com \
       sqladmin.googleapis.com \
       iam.googleapis.com \
       --project=$PROJECT

Provision a Postgres instance
=============================

Use the ``read`` command to securely assign a value to the ``PASSWORD``
environment variable.

.. code-block:: bash

   $ echo -n "> " && read -s PASSWORD

Then create a Cloud SQL instance and set the password.

.. code-block:: bash

   $ gcloud sql instances create ${PROJECT}-postgres \
       --database-version=POSTGRES_14 \
       --cpu=1 \
       --memory=3840MiB \
       --region=us-west2 \
       --project=$PROJECT
   $ gcloud sql users set-password postgres \
       --instance=${PROJECT}-postgres \
       --password=$PASSWORD \
       --project=$PROJECT

Create a Kubernetes cluster
===========================

Create an empty Kubernetes cluster inside your project.

.. code-block:: bash

   $ gcloud container clusters create ${PROJECT}-k8s \
       --zone=us-west2-a \
       --num-nodes=1 \
       --project=$PROJECT

Configure service account
=========================

Create a new service account, configure its permissions, and generate a
``credentials.json`` file.

.. code-block:: bash

   $ gcloud iam service-accounts create ${PROJECT}-account \
       --project=$PROJECT

   $ MEMBER="${PROJECT}-account@${PROJECT}.iam.gserviceaccount.com"
   $ gcloud projects add-iam-policy-binding $PROJECT \
       --member=serviceAccount:${MEMBER} \
       --role=roles/cloudsql.admin \
       --project=$PROJECT

   $ gcloud iam service-accounts keys create credentials.json \
       --iam-account=${MEMBER}

Then use this ``credentials.json`` to authenticate the Kubernetes CLI tool
``kubectl``.

.. code-block:: bash

   $ kubectl create secret generic cloudsql-instance-credentials \
       --from-file=credentials.json=credentials.json

   $ INSTANCE_CONNECTION_NAME=$(
       gcloud sql instances describe ${PROJECT}-postgres \
           --format="value(connectionName)" \
           --project=$PROJECT
   )

   $ DSN="postgresql://postgres:${PASSWORD}@127.0.0.1:5432"
   $ kubectl create secret generic cloudsql-db-credentials \
       --from-literal=dsn=$DSN \
       --from-literal=password=$PASSWORD \
       --from-literal=instance=${INSTANCE_CONNECTION_NAME}=tcp:5432

Deploy EdgeDB
=============

Download the starter EdgeDB Kubernetes configuration file. This file specifies
a persistent volume, a container running a `Cloud SQL authorization proxy
<https://github.com/GoogleCloudPlatform/cloudsql-proxy>`_, and a container to
run `EdgeDB itself <https://github.com/edgedb/edgedb-docker>`_. It relies on
the secrets we declared in the previous step.

.. code-block:: bash

   $ wget "https://raw.githubusercontent.com\
   /edgedb/edgedb-deploy/dev/gcp/deployment.yaml"

   $ kubectl apply -f deployment.yaml

Ensure the pods are running.

.. code-block:: bash

   $ kubectl get pods
   NAME                     READY   STATUS              RESTARTS   AGE
   edgedb-977b8fdf6-jswlw   0/2     ContainerCreating   0          16s

The ``READY  0/2`` tells us neither of the two pods have finished booting.
Re-run the command until ``2/2`` pods are ``READY``.

If there were errors you can check EdgeDB's logs with:

.. code-block:: bash

   $ kubectl logs deployment/edgedb --container edgedb

Persist TLS Certificate
=======================

Now that our EdgeDB instance is up and running, we need to download a local
copy of its self-signed TLS certificate (which it generated on startup) and
pass it as a secret into Kubernetes. Then we'll redeploy the pods.

.. code-block:: bash

   $ kubectl create secret generic cloudsql-tls-credentials \
       --from-literal=tlskey="$(
           kubectl exec deploy/edgedb -c=edgedb -- \
               edgedb-show-secrets.sh --format=raw EDGEDB_SERVER_TLS_KEY
       )" \
       --from-literal=tlscert="$(
           kubectl exec deploy/edgedb -c=edgedb -- \
               edgedb-show-secrets.sh --format=raw EDGEDB_SERVER_TLS_CERT
       )"

   $ kubectl delete -f deployment.yaml

   $ kubectl apply -f deployment.yaml

Expose EdgeDB
=============

.. code-block:: bash

   $ kubectl expose deploy/edgedb --type LoadBalancer


Get your instance's DSN
=======================

Get the public-facing IP address of your database.

.. code-block:: bash

    $ kubectl get service
    NAME         TYPE           CLUSTER-IP  EXTERNAL-IP   PORT(S)
    edgedb       LoadBalancer   <ip>        <ip>          5656:30841/TCP


Copy and paste the ``EXTERNAL-IP`` associated with the service named
``edgedb``. With this IP address, you can construct your instance's :ref:`DSN
<ref_dsn>`:

.. code-block:: bash

    $ EDGEDB_IP=<copy IP address here>
    $ EDGEDB_DSN="edgedb://edgedb:${PASSWORD}@${EDGEDB_IP}"

To print the final DSN, you can ``echo`` it. Note that you should only run
this command on a computer you trust, like a personal laptop or sandboxed
environment.

.. code-block:: bash

    $ echo $EDGEDB_DSN

The resuling DSN can be used to connect to your instance.
To test it, try opening a REPL:

.. code-block:: bash

    $ edgedb --dsn $EDGEDB_DSN --tls-security insecure
    EdgeDB 3.x (repl 3.x)
    Type \help for help, \quit to quit.
    edgedb> select "hello world!";

In development
--------------

To make this instance easier to work with during local development, create an
alias using ``edgedb instance link``.

.. note::

   The command groups ``edgedb instance`` and ``edgedb project`` are not
   intended to manage production instances.

.. code-block:: bash

    $ echo $PASSWORD | edgedb instance link \
        --dsn $EDGEDB_DSN \
        --password-from-stdin \
        --non-interactive \
        --trust-tls-cert \
        gcp_instance

You can now refer to the remote instance using the alias instance on your
machine called ``gcp_instance``. You can use this alias wherever an instance
name is expected; for instance, you can open a REPL:

.. code-block:: bash

   $ edgedb -I gcp_instance

Or apply migrations:

.. code-block:: bash

   $ edgedb -I gcp_instance migrate

In production
-------------

To connect to this instance in production, set the ``EDGEDB_DSN`` environment
variable wherever you deploy your application server; EdgeDB's client
libraries read the value of this variable to know how to connect to your
instance.

Health Checks
=============

Using an HTTP client, you can perform health checks to monitor the status of
your EdgeDB instance. Learn how to use them with our :ref:`health checks guide
<ref_guide_deployment_health_checks>`.
