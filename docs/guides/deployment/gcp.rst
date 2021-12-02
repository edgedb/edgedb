.. _ref_guide_deployment_aws_aurora_ecs:

=======================================
On Google Cloud Platform and Kubernetes
=======================================

In this guide we show how to deploy EdgeDB on GCP using Cloud SQL and
Kubernetes.

Prerequisites
=============

* Google Cloud account with billing enabled (or a `free trial <gcp-trial_>`_)
* ``gcloud`` CLI (`install <gcloud-intsll_>`_)
* ``kubectl`` CLI (`install <kubectl-install_>`_)

.. _gcp-trial: https://cloud.google.com/free/
.. _gcloud-install: https://cloud.google.com/sdk/
.. _kubectl-install: https://kubernetes.io/docs/tasks/tools/install-kubectl/

Make sure you are logged into google cloud.

.. code-block:: bash

   gcloud init

Create a project
================

If you already have a project you can use your existing project.

.. code-block:: bash

   PROJECT=edgedb

   gcloud projects create $PROJECT

Choose a name
=============

We'll add suffixes to this name to create names for each component in the
deployment.

.. code-block:: bash

   NAME=edgedb

Provision a Postgres instance
=============================

Choose your own secure ``ADMIN_PASSWORD``. This will be the password for both
popostgres and EdgeDB.

.. code-block:: bash

   ADMIN_PASSWORD=supersecret1

   gcloud sql instances create ${NAME}-postgres \
       --database-version=POSTGRES_12 \
       --cpu=1 \
       --memory=3840MiB \
       --region=us-west2 \
       --project=$PROJECT

   gcloud sql users set-password postgres \
       --instance=${NAME}-postgres \
       --password=$ADMIN_PASSWORD \
       --project=$PROJECT

Create a Kubernetes cluster
===========================

Before creating the cluster be sure that the Kubernetes Engine API is enabled .

.. code-block:: bash

   gcloud services enable container.googleapis.com \
       --project=$PROJECT

Create a cluster.

.. code-block:: bash

   gcloud container clusters create ${NAME}-k8s \
       --zone=us-west2-a \
       --num-nodes=1 \
       --project=$PROJECT

Configure Cloud SQL proxy credentials
=====================================

.. code-block:: bash

   gcloud services enable iam.googleapis.com \
       --project=$PROJECT

   gcloud iam service-accounts create ${NAME}-account \
       --project=$PROJECT

   gcloud services enable sqladmin.googleapis.com \
       --project=$PROJECT

   gcloud projects add-iam-policy-binding $PROJECT \
       --member="serviceAccount:${NAME}-account@${PROJECT}.iam.gserviceaccount.com" \
       --role=roles/cloudsql.admin \
       --project=$PROJECT

   gcloud iam service-accounts keys create credentials.json \
       --iam-account=${NAME}-account@${PROJECT}.iam.gserviceaccount.com

   kubectl create secret generic cloudsql-instance-credentials \
       --from-file=credentials.json=credentials.json

   INSTANCE_CONNECTION_NAME=$(
       gcloud sql instances describe ${NAME}-postgres \
           --format="value(connectionName)" \
           --project=$PROJECT
   )

   kubectl create secret generic cloudsql-db-credentials \
       --from-literal=dsn="postgresql://postgres:${ADMIN_PASSWORD}@127.0.0.1:5432" \
       --from-literal=password=$ADMIN_PASSWORD \
       --from-literal=instance=${INSTANCE_CONNECTION_NAME}=tcp:5432

Deploy EdgeDB
=============

.. code-block:: bash

   wget url-for-deployment.yaml  # TBD
   kubectl apply -f deployment.yaml

Ensure the pods are running. It may take a minute for the first boot to finish.

.. code-block:: bash

   kubectl get pods

If there were errors you can check EdgeDB's logs with:

.. code-bloc:: bash

   kubectl logs deployment/edgedb --container edgedb

Persist TLS Certificate
=======================

.. code-block:: bash

   kubectl create secret generic cloudsql-tls-credentials \
       --from-literal=tlskey="$(
           kubectl exec deploy/edgedb -c=edgedb -- \
               edgedb-show-secrets.sh --format=raw EDGEDB_SERVER_TLS_KEY
       )" \
       --from-literal=tlscert="$(
           kubectl exec deploy/edgedb -c=edgedb -- \
               edgedb-show-secrets.sh --format=raw EDGEDB_SERVER_TLS_CERT
       )"

   kubectl delete -f deployment.yaml
   kubectl apply -f deployment.yaml

Expose EdgeDB
=============

.. code-bloc:: bash

   kubectl expose deploy/edgedb --type LoadBalancer


Create a local link to the new EdgeDB instance
==============================================

.. code-block:: bash

   echo $ADMIN_PASSWORD | edgedb instance link \
       --password-from-stdin \
       --non-interactive \
       --trust-tls-cert \
       --host "$(
           kubectl get service \
               --template="{{range .items}}{{if eq .spec.type \"LoadBalancer\"}}{{range .status.loadBalancer.ingress}}{{.ip}}{{end}}{{end}}{{end}}"
       )" \
       google

.. code-block:: bash

   edgedb -I google
