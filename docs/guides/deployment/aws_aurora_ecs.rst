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

We maintain a CloudFormation `template <cf-template_>`_ for easy automated
deployment of EdgeDB in your AWS account.  The template deploys EdgeDB
to a new ECS service and connects it to a newly provisioned Aurora PostgreSQL
cluster.  The EdgeDB instance created by the template is exposed to the
Internet and is protected by TLS and a password you provide.

CloudFormation Web Portal
-------------------------

Click `here <cf-deploy_>`_ to start the deployment process using CloudFormation
portal and follow the prompts.

Once the deployment is complete you can find the host name that you will use to
connect to your new EdgeDB instance in the `AWS console <aws_console_>`_. Be
sure that you have the correct region selected (top right corner of the
screen).  Then highlight a network interface. You can use either the public IP
or the public DNS to connect to your new EdgeDB instance.

To access the EdgeDB instance you've just provisioned from your local machine
run ``edgedb instance link``:

.. code-block:: bash

   $ edgedb instance link \
        --trust-tls-cert \
        --host <ip-or-dns> \
        --port 5656 \
        --user edgedb \
        --database edgedb \
        aws

Don't forget to replace ``<ip-or-dns>`` with the value from the AWS console.
You can now use the EdgeDB instance deployed on AWS as ``aws``, for example:

.. code-block:: bash

   $ edgedb -I aws
   edgedb>

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


Manual Install with CLI
=======================

The following instructions produce a deployment that is very similar to the
CloudFormation option above.

Create a VPC
------------

.. code-block:: bash

    $ read -p "Deployment Name: " NAME
    $ REGION=us-west-2

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

.. code-block:: bash

    $ read -rsp "Password: " PASSWORD

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

To access the EdgeDB instance you've just provisioned on AWS from your local
machine run ``edgedb instance link``:

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
