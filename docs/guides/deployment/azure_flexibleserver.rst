.. _ref_guide_deployment_azure_flexibleserver:

================================
On Azure Postgres FlexibleServer
================================

In this guide we show how to deploy EdgeDB on Azure using Postgres
FlexibleServer as the backend.


Prerequisites
=============

* Valid Azure Subscription with billing enabled or credits (`free trial
  <azure-trial_>`_).
* Azure CLI (`install <azure-install_>`_).

.. _azure-trial: https://azure.microsoft.com/en-us/free/
.. _azure-install: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli


Provision an EdgeDB instance
============================

Login to your azure account.

.. code-block:: bash

   $ az login

Create a new resource group.

.. code-block:: bash

   $ GROUP=my-group-name
   $ az group create --name $GROUP --location westus

Provision a PostgreSQL server.

.. note::

   If you already have a database provisioned you can skip this step.

.. note::

   If you get an error saying ``Specified server name is already used.`` change
   the server name and rerun the command.

.. code-block:: bash

   $ PG_SERVER_NAME=postgres-for-edgedb
   $ read -rsp "Password: " PASSWORD
   $ az postgres flexible-server create \
       --resource-group $GROUP \
       --name $PG_SERVER_NAME \
       --location westus \
       --admin-user edgedb \
       --admin-password $PASSWORD \
       --sku-name Standard_D2s_v3 \
       --version 12 \
       --yes

Allow other azure services access to the postgres instance.

.. code-block:: bash

   $ az postgres flexible-server firewall-rule create \
       --resource-group $GROUP \
       --name $PG_SERVER_NAME \
       --rule-name allow-azure-internal \
       --start-ip-address 0.0.0.0 \
       --end-ip-address 0.0.0.0

Start an EdgeDB container.

.. code-block:: bash

   $ PG_HOST=$(
       az postgres flexible-server list \
         --resource-group $GROUP \
         --query "[?name=='$PG_SERVER_NAME'].fullyQualifiedDomainName | [0]" \
         --output tsv
     )
   $ DSN="postgresql://edgedb:$PASSWORD@$PG_HOST/postgres?sslmode=require"
   $ az container create \
       --resource-group $GROUP \
       --name edgedb-container-group \
       --image edgedb/edgedb:nightly \
       --dns-name-label edgedb \
       --ports 5656 \
       --secure-environment-variables \
         "EDGEDB_SERVER_PASSWORD=$PASSWORD" \
         "EDGEDB_SERVER_BACKEND_DSN=$DSN" \
       --environment-variables \
         EDGEDB_SERVER_TLS_CERT_MODE=generate_self_signed \

Persist the SSL certificate. We have configured EdgeDB to generate a self
signed SSL certificate when it starts. However, if the container is restarted a
new certificate would be generated. To preserve the certificate across failures
or reboots copy the certificate files and use their contents in the
``EDGEDB_SERVER_TLS_KEY`` and ``EDGEDB_SERVER_TLS_CERT`` environment variables.

.. code-block:: bash

   $ key="$( az container exec \
               --resource-group $GROUP \
               --name edgedb-container-group \
               --exec-command "cat /etc/ssl/edgedb/edbprivkey.pem" \
             | tr -d "\r" )"
   $ cert="$( az container exec \
                --resource-group $GROUP \
                --name edgedb-container-group \
                --exec-command "cat /etc/ssl/edgedb/edbtlscert.pem" \
             | tr -d "\r" )"
   $ az container delete \
       --resource-group $GROUP \
       --name edgedb-container-group \
       --yes
   $ az container create \
       --resource-group $GROUP \
       --name edgedb-container-group \
       --image edgedb/edgedb:nightly \
       --dns-name-label edgedb \
       --ports 5656 \
       --secure-environment-variables \
         "EDGEDB_SERVER_BACKEND_DSN=$DSN" \
         "EDGEDB_SERVER_TLS_KEY=$key" \
       --environment-variables \
         "EDGEDB_SERVER_TLS_CERT=$cert"


To access the EdgeDB instance you've just provisioned on Azure from your local
machine link the instance.

.. code-block:: bash

   $ printf $PASSWORD | edgedb instance link \
       --password-from-stdin \
       --non-interactive \
       --trust-tls-cert \
       --host $( \
         az container list \
           --resource-group $GROUP \
           --query "[?name=='edgedb-container-group'].ipAddress.fqdn | [0]" \
           --output tsv ) \
       azure

You can now connect to your instance.

.. code-block:: bash

   $ edgedb -I azure
