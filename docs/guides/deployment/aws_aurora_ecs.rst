.. _ref_guide_deployment_aws_aurora_ecs:

===
AWS
===

:edb-alt-title:  Deploying EdgeDB to AWS

In this guide we show how to deploy EdgeDB on AWS using Amazon Aurora and
Elastic Container Service.

Prerequisites
=============

* AWS account with billing enabled (or a `free trial <aws-trial_>`_)
* (optional) ``aws`` CLI (`install <awscli-install_>`_)

.. _aws-trial: https://aws.amazon.com/free
.. _awscli-install:
   https://docs.aws.amazon.com
   /cli/latest/userguide/getting-started-install.html

Quick Install with CloudFormation
=================================

We maintain a `CloudFormation template <cf-template_>`_ for easy automated
deployment of EdgeDB in your AWS account.  The template deploys EdgeDB
to a new ECS service and connects it to a newly provisioned Aurora PostgreSQL
cluster. The created instance has a public IP address with TLS configured and
is protected by a password you provide.

CloudFormation Web Portal
-------------------------

Click `here <cf-deploy_>`_ to start the deployment process using CloudFormation
portal and follow the prompts. You'll be prompted to provide a value for the
following parameters:

- ``DockerImage``: defaults to the latest version (``edgedb/edgedb``), or you
  can specify a particular tag from the ones published to `Docker Hub
  <https://hub.docker.com/r/edgedb/edgedb/tags>`_.
- ``InstanceName``: ⚠️ Due to limitations with AWS, this must be 22 characters
  or less!
- ``SuperUserPassword``: this will be used as the password for the new EdgeDB
  instance. Keep track of the value you provide.

Once the deployment is complete, follow these steps to find the host name that
has been assigned to your EdgeDB instance:

.. lint-off

1. Open the AWS Console and navigate to CloudFormation > Stacks. Click on the
   newly created Stack.
2. Wait for the status to read ``CREATE_COMPLETE``—it can take 15 minutes or
   more.
3. Once deployment is complete, click the ``Outputs`` tab. The value of
   ``PublicHostname`` is the hostname at which your EdgeDB instance is
   publicly available.
4. Copy the hostname and run the following command to open a REPL to your
   instance.

   .. code-block:: bash

     $ edgedb --dsn edgedb://edgedb:<password>@<hostname> --tls-security insecure
     EdgeDB 3.x
     Type \help for help, \quit to quit.
     edgedb>

.. lint-on

It's often convenient to create an alias for the remote instance using
``edgedb instance link``.

.. code-block:: bash

   $ edgedb instance link \
        --trust-tls-cert \
        --dsn edgedb://edgedb:<password>@<hostname>
        my_aws_instance

This aliases the remote instance to ``my_aws_instance`` (this name can be
anything). You can now use the ``-I my_aws_instance`` flag to run CLI commands
against this instance, as with local instances.

.. note::

   The command groups ``edgedb instance`` and ``edgedb project`` are not
   intended to manage production instances.

.. code-block:: bash

  $ edgedb -I my_aws_instance
  EdgeDB 3.x
  Type \help for help, \quit to quit.
  edgedb>

To make changes to your EdgeDB deployment like upgrading the EdgeDB version or
enabling the UI you can follow the CloudFormation
`Updating a stack <stack-update_>`_ instructions. Search for
``ContainerDefinitions`` in the template and you will find where EdgeDB's
:ref:`environment variables <ref_guides_deployment_docker_customization>` are
defined. To upgrade the EdgeDB version specify a
`docker image tag <docker-tags_>`_ with the image name ``edgedb/edgedb`` in the
second step of the update workflow.

CloudFormation CLI
------------------

Alternatively, if you prefer to use AWS CLI, run the following command in
your terminal:

.. code-block:: bash

    $ aws cloudformation create-stack \
        --stack-name EdgeDB \
        --template-url \
          https://edgedb-deploy.s3.us-east-2.amazonaws.com/edgedb-aurora.yml \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters ParameterKey=SuperUserPassword,ParameterValue=<password>


.. _cf-template: https://github.com/edgedb/edgedb-deploy/tree/dev/aws-cf
.. _cf-deploy:
   https://console.aws.amazon.com
   /cloudformation/home#/stacks/new?stackName=EdgeDB&templateURL=
   https%3A%2F%2Fedgedb-deploy.s3.us-east-2.amazonaws.com%2Fedgedb-aurora.yml
.. _aws_console:
   https://console.aws.amazon.com
   /ec2/v2/home#NIC:search=ec2-security-group
.. _stack-update:
   https://docs.aws.amazon.com
   /AWSCloudFormation/latest/UserGuide/cfn-whatis-howdoesitwork.html
.. _docker-tags: https://hub.docker.com/r/edgedb/edgedb/tags


Manual Install with CLI
=======================

The following instructions produce a deployment that is very similar to the
CloudFormation option above.

Create a VPC
------------

For convenience, assign a deployment name and region to environment variables.
The ``NAME`` variable will be used as prefix for all the resources created
throughout the process. It should only contain alphanumeric characters and
hyphens.

.. code-block::

    $ NAME=your-deployment-name
    $ REGION=us-west-2

Then create the VPC.

.. code-block:: bash

    $ VPC_ID=$( \
        aws ec2 create-vpc \
          --region $REGION \
          --output text \
          --query "Vpc.VpcId" \
          --cidr-block "10.0.0.0/16" \
          --instance-tenancy default \
          --tag-specifications \
            "ResourceType=vpc,Tags=[{Key=Name,Value=${NAME}-vpc}]" \
      )

    $ aws ec2 modify-vpc-attribute \
        --region $REGION \
        --vpc-id $VPC_ID \
        --enable-dns-support

    $ aws ec2 modify-vpc-attribute \
        --region $REGION \
        --vpc-id $VPC_ID \
        --enable-dns-hostnames

Create a Gateway
----------------

Allow communication between the VPC and the internet by creating an Internet
Gateway.

.. code-block:: bash

    $ GATEWAY_ID=$( \
        aws ec2 create-internet-gateway \
          --region $REGION \
          --output text \
          --query "InternetGateway.InternetGatewayId" \
          --tag-specifications \
            "ResourceType=internet-gateway, \
             Tags=[{Key=Name,Value=${NAME}-internet-gateway}]" \
      )

    $ aws ec2 attach-internet-gateway \
        --region $REGION \
        --internet-gateway-id $GATEWAY_ID \
        --vpc-id $VPC_ID

Create a Public Network ACL
---------------------------

A Network Access Control List will act as a firewall for a publicly accessible
subnet.

.. code-block:: bash

    $ PUBLIC_ACL_ID=$( \
        aws ec2 create-network-acl \
          --region $REGION \
          --output text \
          --query "NetworkAcl.NetworkAclId" \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=network-acl, \
             Tags=[{Key=Name,Value=${NAME}-public-network-acl}]" \
      )

    $ aws ec2 create-network-acl-entry \
        --region $REGION \
        --network-acl-id $PUBLIC_ACL_ID \
        --rule-number 99 \
        --protocol 6 \
        --port-range From=0,To=65535 \
        --rule-action allow \
        --ingress \
        --cidr-block 0.0.0.0/0

    $ aws ec2 create-network-acl-entry \
        --region $REGION \
        --network-acl-id $PUBLIC_ACL_ID \
        --rule-number 99 \
        --protocol 6 \
        --port-range From=0,To=65535 \
        --rule-action allow \
        --egress \
        --cidr-block 0.0.0.0/0

Create a Private Network ACL
----------------------------

A second ACL will be the firewall for a private subnet to provide an extra
boundary around the PostgreSQL cluster.

.. code-block:: bash

    $ PRIVATE_ACL_ID="$( \
        aws ec2 create-network-acl \
          --region $REGION \
          --output text \
          --query "NetworkAcl.NetworkAclId" \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=network-acl, \
             Tags=[{Key=Name,Value=${NAME}-private-network-acl}]" \
      )"

    $ aws ec2 create-network-acl-entry \
        --region $REGION \
        --network-acl-id $PRIVATE_ACL_ID \
        --rule-number 99 \
        --protocol -1 \
        --rule-action allow \
        --ingress \
        --cidr-block 0.0.0.0/0

    $ aws ec2 create-network-acl-entry \
        --region $REGION \
        --network-acl-id $PRIVATE_ACL_ID \
        --rule-number 99 \
        --protocol -1 \
        --rule-action allow \
        --egress \
        --cidr-block 0.0.0.0/0

Create a Public Subnet in Availability Zone "A"
-----------------------------------------------

.. code-block:: bash

    $ AVAILABILITY_ZONE_A="$( \
        aws ec2 describe-availability-zones \
          --region $REGION \
          --output text \
          --query "AvailabilityZones[0].ZoneName" \
      )"

    $ SUBNET_A_PUBLIC_ID=$( \
        aws ec2 create-subnet \
          --region $REGION \
          --output text \
          --query "Subnet.SubnetId" \
          --availability-zone $AVAILABILITY_ZONE_A \
          --cidr-block 10.0.0.0/20 \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=subnet, \
             Tags=[{Key=Name,Value=${NAME}-subnet-a-public}, \
                   {Key=Reach,Value=public}]" \
      )

    $ aws ec2 replace-network-acl-association \
        --region $REGION \
        --network-acl-id $PUBLIC_ACL_ID \
        --association-id $( \
          aws ec2 describe-network-acls \
            --region $REGION \
            --output text \
            --query " \
            NetworkAcls[*].Associations[?SubnetId=='${SUBNET_A_PUBLIC_ID}'][] \
            | [0].NetworkAclAssociationId" \
        )

    $ ROUTE_TABLE_A_PUBLIC_ID=$( \
        aws ec2 create-route-table \
          --region $REGION \
          --output text \
          --query "RouteTable.RouteTableId" \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=route-table, \
             Tags=[{Key=Name,Value=${NAME}-route-table-a-public}]" \
      )

    $ aws ec2 create-route \
        --region $REGION \
        --route-table-id $ROUTE_TABLE_A_PUBLIC_ID \
        --destination-cidr-block 0.0.0.0/0 \
        --gateway-id $GATEWAY_ID

    $ aws ec2 associate-route-table \
        --region $REGION \
        --route-table-id $ROUTE_TABLE_A_PUBLIC_ID \
        --subnet-id $SUBNET_A_PUBLIC_ID

Create a Private Subnet in Availability Zone "A"
------------------------------------------------

.. code-block:: bash

    $ SUBNET_A_PRIVATE_ID=$( \
        aws ec2 create-subnet \
          --region $REGION \
          --output text \
          --query "Subnet.SubnetId" \
          --availability-zone $AVAILABILITY_ZONE_A \
          --cidr-block 10.0.16.0/20 \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=subnet, \
             Tags=[{Key=Name,Value=${NAME}-subnet-a-private}, \
                   {Key=Reach,Value=private}]" \
      )

    $ aws ec2 replace-network-acl-association \
        --region $REGION \
        --network-acl-id $PRIVATE_ACL_ID \
        --association-id $( \
          aws ec2 describe-network-acls \
            --region $REGION \
            --output text \
            --query " \
            NetworkAcls[*].Associations[?SubnetId == '${SUBNET_A_PRIVATE_ID}' \
            ][] | [0].NetworkAclAssociationId" \
        )

    $ ROUTE_TABLE_A_PRIVATE_ID=$( \
        aws ec2 create-route-table \
          --region $REGION \
          --output text \
          --query "RouteTable.RouteTableId" \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=route-table, \
             Tags=[{Key=Name,Value=${NAME}-route-table-a-private}]" \
      )

    $ aws ec2 associate-route-table \
        --region $REGION \
        --route-table-id $ROUTE_TABLE_A_PRIVATE_ID \
        --subnet-id $SUBNET_A_PRIVATE_ID

Create a Public Subnet in Availability Zone "B"
-----------------------------------------------

.. code-block:: bash

    $ AVAILABILITY_ZONE_B="$( \
        aws ec2 describe-availability-zones \
          --region $REGION \
          --output text \
          --query "AvailabilityZones[1].ZoneName" \
      )"

    $ SUBNET_B_PUBLIC_ID=$( \
        aws ec2 create-subnet \
          --region $REGION \
          --output text \
          --query "Subnet.SubnetId" \
          --availability-zone $AVAILABILITY_ZONE_B \
          --cidr-block 10.0.32.0/20 \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=subnet, \
             Tags=[{Key=Name,Value=${NAME}-subnet-b-public}, \
                   {Key=Reach,Value=public}]" \
      )

    $ aws ec2 replace-network-acl-association \
        --region $REGION \
        --network-acl-id $PUBLIC_ACL_ID \
        --association-id $( \
          aws ec2 describe-network-acls \
            --region $REGION \
            --output text \
            --query " \
              NetworkAcls[*].Associations[?SubnetId == '${SUBNET_B_PUBLIC_ID}'\
              ][] | [0].NetworkAclAssociationId" \
        )

    $ ROUTE_TABLE_B_PUBLIC_ID=$( \
        aws ec2 create-route-table \
          --region $REGION \
          --output text \
          --query "RouteTable.RouteTableId" \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=route-table, \
             Tags=[{Key=Name,Value=${NAME}-route-table-b-public}]" \
      )

    $ aws ec2 create-route \
        --region $REGION \
        --route-table-id $ROUTE_TABLE_B_PUBLIC_ID \
        --destination-cidr-block 0.0.0.0/0 \
        --gateway-id $GATEWAY_ID

    $ aws ec2 associate-route-table \
        --region $REGION \
        --route-table-id $ROUTE_TABLE_B_PUBLIC_ID \
        --subnet-id $SUBNET_B_PUBLIC_ID

Create a Private Subnet in Availability Zone "B"
------------------------------------------------

.. code-block:: bash

   $ SUBNET_B_PRIVATE_ID=$( \
       aws ec2 create-subnet \
         --region $REGION \
         --output text \
         --query "Subnet.SubnetId" \
         --availability-zone $AVAILABILITY_ZONE_B \
         --cidr-block 10.0.48.0/20 \
         --vpc-id $VPC_ID \
         --tag-specifications \
           "ResourceType=subnet, \
            Tags=[{Key=Name,Value=${NAME}-subnet-b-private}, \
                  {Key=Reach,Value=private}]" \
     )

   $ aws ec2 replace-network-acl-association \
       --region $REGION \
       --network-acl-id $PRIVATE_ACL_ID \
       --association-id $( \
         aws ec2 describe-network-acls \
           --region $REGION \
           --output text \
           --query " \
           NetworkAcls[*].Associations[?SubnetId=='${SUBNET_B_PRIVATE_ID}'][] \
           | [0].NetworkAclAssociationId" \
       )

   $ ROUTE_TABLE_B_PRIVATE_ID=$( \
       aws ec2 create-route-table \
         --region $REGION \
         --output text \
         --query "RouteTable.RouteTableId" \
         --vpc-id $VPC_ID \
         --tag-specifications \
           "ResourceType=route-table, \
            Tags=[{Key=Name,Value=${NAME}-route-table-b-private}]" \
     )

   $ aws ec2 associate-route-table \
       --region $REGION \
       --route-table-id $ROUTE_TABLE_B_PRIVATE_ID \
       --subnet-id $SUBNET_B_PRIVATE_ID

Create an EC2 security group
----------------------------

.. code-block:: bash

    $ EC2_SECURITY_GROUP_ID=$( \
        aws ec2 create-security-group \
          --region $REGION \
          --output text \
          --query "GroupId" \
          --group-name "${NAME}-ec2-security-group" \
          --description "Controls access to ${NAME} stack EC2 instances." \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=security-group, \
             Tags=[{Key=Name,Value=${NAME}-ec2-security-group}]" \
      )

    $ aws ec2 authorize-security-group-ingress \
        --region $REGION \
        --group-id $EC2_SECURITY_GROUP_ID \
        --protocol tcp \
        --cidr 0.0.0.0/0 \
        --port 5656 \
        --tag-specifications \
          "ResourceType=security-group-rule, \
           Tags=[{Key=Name,Value=${NAME}-ec2-security-group-ingress}]"

Create an RDS Security Group
----------------------------

.. code-block:: bash

    $ RDS_SECURITY_GROUP_ID=$( \
        aws ec2 create-security-group \
          --region $REGION \
          --output text \
          --query "GroupId" \
          --group-name "${NAME}-rds-security-group" \
          --description "Controls access to ${NAME} stack RDS instances." \
          --vpc-id $VPC_ID \
          --tag-specifications \
            "ResourceType=security-group, \
             Tags=[{Key=Name,Value=${NAME}-rds-security-group}]" \
      )

    $ aws ec2 authorize-security-group-ingress \
        --region $REGION \
        --group-id $RDS_SECURITY_GROUP_ID \
        --protocol tcp \
        --source-group $EC2_SECURITY_GROUP_ID \
        --port 5432 \
        --tag-specifications \
          "ResourceType=security-group-rule, \
           Tags=[{Key=Name,Value=${NAME}-rds-security-group-ingress}]"

    $ RDS_SUBNET_GROUP_NAME="${NAME}-rds-subnet-group"

    $ aws rds create-db-subnet-group \
        --region $REGION \
        --db-subnet-group-name "$RDS_SUBNET_GROUP_NAME" \
        --db-subnet-group-description "EdgeDB RDS subnet group for ${NAME}" \
        --subnet-ids $SUBNET_A_PRIVATE_ID $SUBNET_B_PRIVATE_ID

Create an RDS Cluster
---------------------


Use the ``read`` command to securely assign a value to the
``PASSWORD`` environment variable.

.. code-block:: bash

   $ echo -n "> " && read -s PASSWORD

Then use this password to create an AWS `secret
<https://aws.amazon.com/secrets-manager/>`_.

.. code-block:: bash

    $ PASSWORD_ARN="$( \
        aws secretsmanager create-secret \
          --region $REGION \
          --output text \
          --query "ARN" \
          --name "${NAME}-password" \
          --secret-string "$PASSWORD" \
      )"

    $ DB_CLUSTER_IDENTIFIER="${NAME}-postgres-cluster"

    $ DB_CLUSTER_ADDRESS="$( \
        aws rds create-db-cluster \
          --region $REGION \
          --output text \
          --query "DBCluster.Endpoint" \
          --engine aurora-postgresql \
          --engine-version 13.4 \
          --db-cluster-identifier "$DB_CLUSTER_IDENTIFIER" \
          --db-subnet-group-name "$RDS_SUBNET_GROUP_NAME" \
          --master-username postgres \
          --master-user-password "$PASSWORD" \
          --port 5432 \
          --vpc-security-group-ids "$RDS_SECURITY_GROUP_ID" \
      )"

    $ aws rds create-db-instance \
        --region $REGION \
        --availability-zone "$AVAILABILITY_ZONE_A" \
        --engine "aurora-postgresql" \
        --db-cluster-identifier "$DB_CLUSTER_IDENTIFIER" \
        --db-instance-identifier "${NAME}-postgres-instance-a" \
        --db-instance-class "db.t3.medium" \
        --db-subnet-group-name "$RDS_SUBNET_GROUP_NAME"

    $ aws rds create-db-instance \
        --region $REGION \
        --availability-zone "$AVAILABILITY_ZONE_B" \
        --engine "aurora-postgresql" \
        --db-cluster-identifier "$DB_CLUSTER_IDENTIFIER" \
        --db-instance-identifier "${NAME}-postgres-instance-b" \
        --db-instance-class "db.t3.medium" \
        --db-subnet-group-name "$RDS_SUBNET_GROUP_NAME"

    $ DSN_ARN="$( \
        aws secretsmanager create-secret \
          --region $REGION \
          --output text \
          --query "ARN" \
          --name "${NAME}-backend-dsn" \
          --secret-string \
        "postgres://postgres:${PASSWORD}@${DB_CLUSTER_ADDRESS}:5432/postgres" \
      )"

Create a Load Balancer
----------------------

Adding a load balancer will facilitate scaling the EdgeDB cluster.


.. code-block:: bash

    $ TARGET_GROUP_ARN="$( \
        aws elbv2 create-target-group \
          --region $REGION \
          --output text \
          --query "TargetGroups[0].TargetGroupArn" \
          --health-check-interval-seconds 10 \
          --health-check-path "/server/status/ready" \
          --health-check-protocol HTTPS \
          --unhealthy-threshold-count 2 \
          --healthy-threshold-count 2 \
          --name "${NAME}-target-group" \
          --port 5656 \
          --protocol TCP \
          --target-type ip \
          --vpc-id $VPC_ID \
      )"

    $ LOAD_BALANCER_NAME="${NAME}-load-balancer"

    $ LOAD_BALANCER_ARN="$( \
        aws elbv2 create-load-balancer \
          --region $REGION \
          --output text \
          --query "LoadBalancers[0].LoadBalancerArn" \
          --type network \
          --name "$LOAD_BALANCER_NAME" \
          --scheme internet-facing \
          --subnets "$SUBNET_A_PUBLIC_ID" "$SUBNET_B_PUBLIC_ID" \
      )"

    $ aws elbv2 create-listener \
        --region $REGION \
        --default-actions \
          '[{"TargetGroupArn": "'"$TARGET_GROUP_ARN"'","Type": "forward"}]' \
        --load-balancer-arn "$LOAD_BALANCER_ARN" \
        --port 5656 \
        --protocol TCP

Create an ECS Cluster
---------------------

The only thing left to do is create and ECS cluster and deploy the EdgeDB
container in it.

.. code-block:: bash

    $ EXECUTION_ROLE_NAME="${NAME}-execution-role"

    $ EXECUTION_ROLE_ARN="$( \
        aws iam create-role \
          --region $REGION \
          --output text \
          --query "Role.Arn" \
          --role-name "$EXECUTION_ROLE_NAME" \
          --assume-role-policy-document \
            "{ \
              \"Version\": \"2012-10-17\", \
              \"Statement\": [{ \
                \"Effect\": \"Allow\", \
                \"Principal\": {\"Service\": \"ecs-tasks.amazonaws.com\"}, \
                \"Action\": \"sts:AssumeRole\" \
              }] \
            }" \
      )"

    $ SECRETS_ACCESS_POLICY_ARN="$( \
        aws iam create-policy \
          --region $REGION \
          --output text \
          --query "Policy.Arn" \
          --policy-name "${NAME}-secrets-access-policy" \
          --policy-document \
            "{ \
              \"Version\": \"2012-10-17\", \
              \"Statement\": [{ \
                \"Effect\": \"Allow\", \
                \"Action\": \"secretsmanager:GetSecretValue\", \
                \"Resource\": [ \
                  \"$PASSWORD_ARN\", \
                  \"$DSN_ARN\" \
                ] \
              }] \
            }" \
      )"

    $ aws iam attach-role-policy \
        --region $REGION \
        --role-name "$EXECUTION_ROLE_NAME" \
        --policy-arn \
        "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"

    $ aws iam attach-role-policy \
        --region $REGION \
        --role-name "$EXECUTION_ROLE_NAME" \
        --policy-arn "$SECRETS_ACCESS_POLICY_ARN"

    $ TASK_ROLE_ARN="$( \
        aws iam create-role \
          --region $REGION \
          --output text \
          --query "Role.Arn" \
          --role-name "${NAME}-task-role" \
          --assume-role-policy-document \
            "{ \
              \"Version\": \"2012-10-17\", \
              \"Statement\": [{ \
                \"Effect\": \"Allow\",  \
                \"Principal\": {\"Service\": \"ecs-tasks.amazonaws.com\"}, \
                \"Action\": \"sts:AssumeRole\" \
              }] \
            }" \
      )"

    $ LOG_GROUP_NAME="/ecs/edgedb/$NAME"

    $ aws logs create-log-group \
        --region $REGION \
        --log-group-name "$LOG_GROUP_NAME"

    $ CLUSTER_NAME="${NAME}-server-cluster"

    $ aws ecs create-cluster \
        --region $REGION \
        --cluster-name "$CLUSTER_NAME"

    $ LOG_GROUP_ARN="$( \
        aws logs describe-log-groups \
          --region $REGION \
          --output text \
          --query "logGroups[0].arn" \
          --log-group-name-prefix "$LOG_GROUP_NAME" \
      )"

    $ TASK_DEFINITION_ARN="$( \
        aws ecs register-task-definition \
          --region $REGION \
          --output text \
          --query "taskDefinition.taskDefinitionArn" \
          --requires-compatibilities "FARGATE" \
          --network-mode "awsvpc" \
          --execution-role-arn "$EXECUTION_ROLE_ARN" \
          --task-role-arn "$TASK_ROLE_ARN" \
          --family "${NAME}-task-definition" \
          --cpu 1024 \
          --memory 2GB \
          --container-definitions \
            "[{ \
              \"name\": \"$NAME\", \
              \"image\": \"edgedb/edgedb\", \
              \"portMappings\": [{\"containerPort\": 5656}], \
              \"command\": [\"edgedb-server\"], \
              \"environment\": [{ \
                \"name\": \"EDGEDB_SERVER_GENERATE_SELF_SIGNED_CERT\", \
                \"value\": \"1\" \
              }], \
              \"secrets\": [ \
                { \
                  \"name\": \"EDGEDB_SERVER_PASSWORD\", \
                  \"valueFrom\": \"$PASSWORD_ARN\" \
                }, \
                { \
                  \"name\": \"EDGEDB_SERVER_BACKEND_DSN\", \
                  \"valueFrom\": \"$DSN_ARN\" \
                } \
              ], \
              \"logConfiguration\": { \
                \"logDriver\": \"awslogs\", \
                \"options\": { \
                  \"awslogs-region\": \"$REGION\", \
                  \"awslogs-group\": \"$LOG_GROUP_NAME\", \
                  \"awslogs-stream-prefix\": \"ecs\" \
                } \
              } \
            }]" \
      )"

    $ aws ecs create-service \
        --region $REGION \
        --service-name "$NAME" \
        --cluster "$CLUSTER_NAME" \
        --task-definition "$TASK_DEFINITION_ARN" \
        --deployment-configuration \
          "minimumHealthyPercent=100,maximumPercent=200" \
        --desired-count 2 \
        --health-check-grace-period-seconds 120 \
        --launch-type FARGATE \
        --network-configuration \
          "awsvpcConfiguration={ \
            assignPublicIp=ENABLED, \
            subnets=[$SUBNET_A_PUBLIC_ID,$SUBNET_B_PUBLIC_ID], \
            securityGroups=[$EC2_SECURITY_GROUP_ID] \
          }" \
        --load-balancers \
          "containerName=$NAME, \
           containerPort=5656, \
           targetGroupArn=$TARGET_GROUP_ARN"

Create a local link to the new EdgeDB instance
----------------------------------------------

Create an local alias to the remote EdgeDB instance with ``edgedb instance
link``:

.. code-block:: bash

    $ printf $PASSWORD | edgedb instance link \
        --password-from-stdin \
        --trust-tls-cert \
        --non-interactive \
        --host "$( \
          aws ec2 describe-network-interfaces \
            --output text \
            --region $REGION \
            --query \
            "NetworkInterfaces[?contains(Description, '$LOAD_BALANCER_NAME')] \
            | [0].Association.PublicIp" \
        )" \
        aws

.. note::

   The command groups ``edgedb instance`` and ``edgedb project`` are not
   intended to manage production instances.

You can now open a REPL to this instance

Health Checks
=============

Using an HTTP client, you can perform health checks to monitor the status of
your EdgeDB instance. Learn how to use them with our :ref:`health checks guide
<ref_guide_deployment_health_checks>`.
