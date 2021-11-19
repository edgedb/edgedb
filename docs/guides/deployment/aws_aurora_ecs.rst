.. _ref_guide_deployment_aws_aurora_ecs:

=====================
On AWS Aurora and ECS
=====================

In this guide we show how to deploy EdgeDB on AWS using Amazon Aurora and
Elastic Container Service.

Prerequisites
=============

* AWS account with billing enabled (or a `free trial <aws-trial_>`_)
* (optional) ``aws`` CLI (`install <awscli-install_>`_)

.. _aws-trial: https://aws.amazon.com/free
.. _awscli-install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html


Quick Install with CloudFormation
=================================

We maintain a CloudFormation `template <cf-template_>`_ for easy automated
deployment of EdgeDB in your AWS account.  The template deploys EdgeDB
to a new ECS service and connects it to a newly provisioned Aurora PostgreSQL
cluster.  The EdgeDB instance created by the template is exposed to the
Internet and is protected by TLS and a password you provide.

CloudFormation Web Portal
-------------------------

Click `here <cf-deploy_>`_ to start the deployment process using CloudFormation
portal and follow the prompts.

CloudFormation CLI
------------------

Alternatively, if you prefer to use AWS CLI, run the following command in
your terminal:

.. code-block:: bash

    $ aws cloudformation create-stack \
        --stack-name EdgeDB \
        --template-url https://edgedb-deploy.s3.us-east-2.amazonaws.com/edgedb-aurora.yml \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters ParameterKey=SuperUserPassword,ParameterValue=<password>


.. _cf-template: https://github.com/edgedb/edgedb-deploy/tree/dev/aws-cf
.. _cf-deploy: https://console.aws.amazon.com/cloudformation/home#/stacks/new?stackName=EdgeDB&templateURL=https%3A%2F%2Fedgedb-deploy.s3.us-east-2.amazonaws.com%2Fedgedb-aurora.yml
