.. _ref_guide_deployment_azure_flexibleserver:

=====
Azure
=====

:edb-alt-title: Deploying Gel to Azure

In this guide we show how to deploy Gel using Azure's `Postgres
Flexible Server
<https://docs.microsoft.com/en-us/azure/postgresql/flexible-server>`_ as the
backend.

Prerequisites
=============

* Valid Azure Subscription with billing enabled or credits (`free trial
  <azure-trial_>`_).
* Azure CLI (`install <azure-install_>`_).

.. _azure-trial: https://azure.microsoft.com/en-us/free/
.. _azure-install: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli


Provision an Gel instance
=========================

Login to your Microsoft Azure account.

.. code-block:: bash

   $ az login

Create a new resource group.

.. code-block:: bash

   $ GROUP=my-group-name
   $ az group create --name $GROUP --location westus

Provision a PostgreSQL server.

.. note::

   If you already have a database provisioned you can skip this step.

For convenience, assign a value to the ``PG_SERVER_NAME`` environment
variable; we'll use this variable in multiple later commands.

.. code-block:: bash

   $ PG_SERVER_NAME=postgres-for-gel

Use the ``read`` command to securely assign a value to the ``PASSWORD``
environment variable.

.. code-block:: bash

   $ echo -n "> " && read -s PASSWORD

Then create a Postgres Flexible server.

.. code-block:: bash

   $ az postgres flexible-server create \
       --resource-group $GROUP \
       --name $PG_SERVER_NAME \
       --location westus \
       --admin-user admin \
       --admin-password $PASSWORD \
       --sku-name Standard_D2s_v3 \
       --version 14 \
       --yes

.. note::

   If you get an error saying ``"Specified server name is already used."``
   change the value of ``PG_SERVER_NAME`` and rerun the command.

Allow other Azure services access to the Postgres instance.

.. code-block:: bash

   $ az postgres flexible-server firewall-rule create \
       --resource-group $GROUP \
       --name $PG_SERVER_NAME \
       --rule-name allow-azure-internal \
       --start-ip-address 0.0.0.0 \
       --end-ip-address 0.0.0.0

|Gel| requires Postgres' ``uuid-ossp`` extension which needs to be enabled.

.. code-block:: bash

   $ az postgres flexible-server parameter set \
       --resource-group $GROUP \
       --server-name $PG_SERVER_NAME \
       --name azure.extensions \
       --value uuid-ossp

Start an Gel container.

.. code-block:: bash

   $ PG_HOST=$(
       az postgres flexible-server list \
         --resource-group $GROUP \
         --query "[?name=='$PG_SERVER_NAME'].fullyQualifiedDomainName | [0]" \
         --output tsv
     )
   $ DSN="postgresql://gel:$PASSWORD@$PG_HOST/postgres?sslmode=require"
   $ az container create \
       --resource-group $GROUP \
       --name gel-container-group \
       --image geldata/gel \
       --dns-name-label gel \
       --ports 5656 \
       --secure-environment-variables \
         "GEL_SERVER_PASSWORD=$PASSWORD" \
         "GEL_SERVER_BACKEND_DSN=$DSN" \
       --environment-variables \
         GEL_SERVER_TLS_CERT_MODE=generate_self_signed \

Persist the SSL certificate. We have configured Gel to generate a self
signed SSL certificate when it starts. However, if the container is restarted a
new certificate would be generated. To preserve the certificate across failures
or reboots copy the certificate files and use their contents in the
:gelenv:`SERVER_TLS_KEY` and :gelenv:`SERVER_TLS_CERT` environment variables.

.. code-block:: bash

   $ key="$( az container exec \
               --resource-group $GROUP \
               --name gel-container-group \
               --exec-command "cat /tmp/gel/edbprivkey.pem" \
             | tr -d "\r" )"
   $ cert="$( az container exec \
                --resource-group $GROUP \
                --name gel-container-group \
                --exec-command "cat /tmp/gel/edbtlscert.pem" \
             | tr -d "\r" )"
   $ az container delete \
       --resource-group $GROUP \
       --name gel-container-group \
       --yes
   $ az container create \
       --resource-group $GROUP \
       --name gel-container-group \
       --image geldata/gel \
       --dns-name-label gel \
       --ports 5656 \
       --secure-environment-variables \
         "GEL_SERVER_PASSWORD=$PASSWORD" \
         "GEL_SERVER_BACKEND_DSN=$DSN" \
         "GEL_SERVER_TLS_KEY=$key" \
       --environment-variables \
         "GEL_SERVER_TLS_CERT=$cert"


To access the Gel instance you've just provisioned on Azure from your local
machine link the instance.

.. code-block:: bash

   $ printf $PASSWORD | gel instance link \
       --password-from-stdin \
       --non-interactive \
       --trust-tls-cert \
       --host $( \
         az container list \
           --resource-group $GROUP \
           --query "[?name=='gel-container-group'].ipAddress.fqdn | [0]" \
           --output tsv ) \
       azure

.. note::

   The command groups :gelcmd:`instance` and :gelcmd:`project` are not
   intended to manage production instances.

You can now connect to your instance.

.. code-block:: bash

   $ gel -I azure

Health Checks
=============

Using an HTTP client, you can perform health checks to monitor the status of
your Gel instance. Learn how to use them with our :ref:`health checks guide
<ref_guide_deployment_health_checks>`.
