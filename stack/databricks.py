from aws_cdk import (
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    Aws,
    CustomResource,
    Duration,
    RemovalPolicy,
    Stack,
)
from constructs import Construct
import base64
import datetime
import json
import random
import textwrap

class Databricks(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        db_account_id = #'FILLMEIN'
        username = #'FILLMEIN'
        password = #'FILLMEIN'
        workspace_name = #'FILLMEIN'


        db_aws_account_id = "414351767826"

        #####
        # Set up databricks cross-account role
        #####

        db_ec2_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'ec2:AllocateAddress',
                'ec2:AssociateDhcpOptions',
                'ec2:AssociateIamInstanceProfile',
                'ec2:AssociateRouteTable',
                'ec2:AttachInternetGateway',
                'ec2:AttachVolume',
                'ec2:AuthorizeSecurityGroupEgress',
                'ec2:AuthorizeSecurityGroupIngress',
                'ec2:CancelSpotInstanceRequests',
                'ec2:CreateDhcpOptions',
                'ec2:CreateInternetGateway',
                'ec2:CreateKeyPair',
                'ec2:CreateNatGateway',
                'ec2:CreatePlacementGroup',
                'ec2:CreateRoute',
                'ec2:CreateRouteTable',
                'ec2:CreateSecurityGroup',
                'ec2:CreateSubnet',
                'ec2:CreateTags',
                'ec2:CreateVolume',
                'ec2:CreateVpc',
                'ec2:CreateVpcEndpoint',
                'ec2:DeleteDhcpOptions',
                'ec2:DeleteInternetGateway',
                'ec2:DeleteKeyPair',
                'ec2:DeleteNatGateway',
                'ec2:DeletePlacementGroup',
                'ec2:DeleteRoute',
                'ec2:DeleteRouteTable',
                'ec2:DeleteSecurityGroup',
                'ec2:DeleteSubnet',
                'ec2:DeleteTags',
                'ec2:DeleteVolume',
                'ec2:DeleteVpc',
                'ec2:DeleteVpcEndpoints',
                'ec2:DescribeAvailabilityZones',
                'ec2:DescribeIamInstanceProfileAssociations',
                'ec2:DescribeInstanceStatus',
                'ec2:DescribeInstances',
                'ec2:DescribeInternetGateways',
                'ec2:DescribeNatGateways',
                'ec2:DescribePlacementGroups',
                'ec2:DescribePrefixLists',
                'ec2:DescribeReservedInstancesOfferings',
                'ec2:DescribeRouteTables',
                'ec2:DescribeSecurityGroups',
                'ec2:DescribeSpotInstanceRequests',
                'ec2:DescribeSpotPriceHistory',
                'ec2:DescribeSubnets',
                'ec2:DescribeVolumes',
                'ec2:DescribeVpcs',
                'ec2:DetachInternetGateway',
                'ec2:DisassociateIamInstanceProfile',
                'ec2:DisassociateRouteTable',
                'ec2:ModifyVpcAttribute',
                'ec2:ReleaseAddress',
                'ec2:ReplaceIamInstanceProfileAssociation',
                'ec2:ReplaceRoute',
                'ec2:RequestSpotInstances',
                'ec2:RevokeSecurityGroupEgress',
                'ec2:RevokeSecurityGroupIngress',
                'ec2:RunInstances',
                'ec2:TerminateInstances',
            ],
            resources=["*"]
        )

        db_ec2_spot_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'iam:CreateServiceLinkedRole',
                'iam:PutRolePolicy',
            ],
            resources=[f"arn:{Aws.PARTITION}:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"],
            conditions={"StringLike":{'iam:AWSServiceName': 'spot.amazonaws.com'}}
        )

        db_x_account_policy_document = iam.PolicyDocument(statements=[
                                                db_ec2_statement,
                                                db_ec2_spot_statement,
                                            ]
                                        )
        db_x_account_assumed_principal = iam.PrincipalWithConditions(
            principal=iam.ArnPrincipal(f"arn:aws:iam::{db_aws_account_id}:root"),
            conditions={"StringEquals":{'sts:ExternalId': db_account_id}}
        )
 
        db_x_account_role = iam.Role(self,
                                     "databricks-cross-account-iam-role",
                                     inline_policies={
                                            'databricks-cross-account-iam-role-policy': db_x_account_policy_document
                                        },
                                     assumed_by=db_x_account_assumed_principal
                                    )

        #####
        # Set up databricks bucket
        #####

        db_bucket_id = 'gen-databricks-root'

        db_bucket = s3.Bucket(
            self,
            id=db_bucket_id,
            bucket_name=db_bucket_id + f"-{Aws.ACCOUNT_ID}",
            access_control=s3.BucketAccessControl.PRIVATE,
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        db_bucket_policy = iam.PolicyStatement(
            sid="Grant Databricks Access",
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
            ],
            principals=[iam.ArnPrincipal(f"arn:aws:iam::{db_aws_account_id}:root")],
            resources=[
                db_bucket.bucket_arn,
                db_bucket.arn_for_objects('*')
            ],
        )

        db_bucket.add_to_resource_policy(db_bucket_policy)

        #####
        # Set up copy zips lambda
        #####

        lambda_zips_bucket_id = 'gen-databricks-lambda'

        lambda_zips_bucket = s3.Bucket(
            self,
            id=lambda_zips_bucket_id,
            bucket_name=lambda_zips_bucket_id + f"-{Aws.ACCOUNT_ID}",
            access_control=s3.BucketAccessControl.PRIVATE,
            encryption=s3.BucketEncryption.S3_MANAGED,
            public_read_access=False,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        copier_statement1 = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                's3:GetObject',
            ],
            resources=[f"arn:{Aws.PARTITION}:s3:::databricks-prod-public-cfts/*"]
        )

        copier_statement2 = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                's3:GetObject',
                's3:PutObject',
                's3:DeleteObject',
            ],
            resources=[f"arn:{Aws.PARTITION}:s3:::{lambda_zips_bucket.bucket_name}/*"]
        )

        lambda_copier_policy_document = iam.PolicyDocument(statements=[
                                                copier_statement1,
                                                copier_statement2,
                                            ]
                                        )
        copy_zips_role = iam.Role(self,
                                  "databricks-copy-zips-role",
                                  inline_policies={
                                         'databricks-lambda-copier': lambda_copier_policy_document
                                     },
                                  assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                 )

        copy_zips_code = textwrap.dedent("""
          import json
          import logging
          import threading
          import boto3
          import cfnresponse
          def copy_objects(source_bucket, dest_bucket, prefix, objects):
              s3 = boto3.client('s3')
              for o in objects:
                  key = prefix + o
                  copy_source = {
                      'Bucket': source_bucket,
                      'Key': key
                  }
                  print('copy_source: %s' % copy_source)
                  print('dest_bucket = %s'%dest_bucket)
                  print('key = %s' %key)
                  s3.copy_object(CopySource=copy_source, Bucket=dest_bucket,
                        Key=key)
          def delete_objects(bucket, prefix, objects):
              s3 = boto3.client('s3')
              objects = {'Objects': [{'Key': prefix + o} for o in objects]}
              s3.delete_objects(Bucket=bucket, Delete=objects)
          def timeout(event, context):
              logging.error('Execution is about to time out, sending failure response to CloudFormation')
              cfnresponse.send(event, context, cfnresponse.FAILED, {}, None)
          def handler(event, context):
              # make sure we send a failure to CloudFormation if the function
              # is going to timeout
              timer = threading.Timer((context.get_remaining_time_in_millis()
                        / 1000.00) - 0.5, timeout, args=[event, context])
              timer.start()
              print('Received event: %s' % json.dumps(event))
              status = cfnresponse.SUCCESS
              try:
                  source_bucket = event['ResourceProperties']['SourceBucket']
                  dest_bucket = event['ResourceProperties']['DestBucket']
                  prefix = event['ResourceProperties']['Prefix']
                  objects = event['ResourceProperties']['Objects']
                  if event['RequestType'] == 'Delete':
                      delete_objects(dest_bucket, prefix, objects)
                  else:
                      copy_objects(source_bucket, dest_bucket, prefix, objects)
              except Exception as e:
                  logging.error('Exception: %s' % e, exc_info=True)
                  status = cfnresponse.FAILED
              finally:
                  timer.cancel()
                  cfnresponse.send(event, context, status, {}, None)
        """)

        copy_zips_function = lambda_.Function(
            self,
            "CopyZipsFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="index.handler",
            code=lambda_.Code.from_inline(copy_zips_code),
            timeout=Duration.seconds(240),
            role=copy_zips_role
        )

        copy_zips = CustomResource(
            self,
            "CopyZips",
            service_token=copy_zips_function.function_arn,
            properties={
                'DestBucket': lambda_zips_bucket.bucket_name,
                'SourceBucket': 'databricks-prod-public-cfts',
                'Prefix': '',
                'Objects': ['functions/packages/default-cluster/lambda.zip']
            }
        )

        #####
        # databricks workspace creation
        #####

        db_function_exec_role = iam.Role(self,
                                         "databricks-function-execution-role",
                                         managed_policies=[
                                           iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                                           iam.ManagedPolicy.from_aws_managed_policy_name("IAMFullAccess")
                                         ],
                                         assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                        )

        api_function = lambda_.Function(
            self,
            "databricksApiFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="rest_client.handler",
            code=lambda_.Code.from_bucket(
                bucket=lambda_zips_bucket,
                key='functions/packages/default-cluster/lambda.zip',
            ),
            timeout=Duration.seconds(900),
            role=db_function_exec_role
        )

        api_function.node.add_dependency(copy_zips)

        create_credentials = CustomResource(
            self,
            "CreateCredentials",
            service_token=api_function.function_arn,
            properties={
                'action': 'CREATE_CREDENTIALS',
                'accountId': db_account_id,
                'credentials_name': workspace_name + '-credentials',
                'role_arn': db_x_account_role.role_arn,
                'encodedbase64': base64.b64encode(username.encode('ascii')+b':'+password.encode('ascii')).decode(),
                'user_agent': 'databricks-CloudFormation-Trial-inhouse-default-cluster'
            }
        )

        create_storage_configuration = CustomResource(
            self,
            "CreateStorageConfigurations",
            service_token=api_function.function_arn,
            properties={
                'action': 'CREATE_STORAGE_CONFIGURATIONS',
                'accountId': db_account_id,
                'storage_config_name': workspace_name + '-storage',
                's3bucket_name': db_bucket.bucket_name,
                'encodedbase64': base64.b64encode(username.encode('ascii')+b':'+password.encode('ascii')).decode(), 
                'user_agent': 'databricks-CloudFormation-Trial-inhouse-default-cluster'
            }
        )

        create_workspace = CustomResource(
            self,
            "CreateWorkspace",
            service_token=api_function.function_arn,
            properties={
                'action': 'CREATE_WORKSPACES',
                'accountId': db_account_id,
                'workspace_name': workspace_name,
                'deployment_name': '',
                'aws_region': Aws.REGION,
                'credentials_id': create_credentials.get_att_string('CredentialsId'),
                'storage_config_id': create_storage_configuration.get_att_string('StorageConfigId'),
                'encodedbase64': base64.b64encode(username.encode('ascii')+b':'+password.encode('ascii')).decode(), 
                'network_id': '',
                'customer_managed_key_id': '',
                'pricing_tier': '',
                'hipaa_parm': '',
                'customer_name': '',
                'authoritative_user_email': '',
                'authoritative_user_full_name': '',
                'user_agent': 'databricks-CloudFormation-Trial-inhouse-default-cluster'
            }
        )
