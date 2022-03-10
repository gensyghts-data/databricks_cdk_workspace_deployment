from aws_cdk import (
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    Aws,
    BundlingOptions,
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

    def __init__(self,
                 scope: Construct,
                 construct_id: str,
                 db_account_id: str,
                 username: str,
                 password: str,
                 workspace_name: str,
                 vpc_id: str,
                 subnet_ids: str,
                 security_group_ids: str,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
                'ec2:DescribeVpcAttribute',
                'ec2:DeleteVpcEndpoints',
                'ec2:DescribeAvailabilityZones',
                'ec2:DescribeIamInstanceProfileAssociations',
                'ec2:DescribeInstanceStatus',
                'ec2:DescribeInstances',
                'ec2:DescribeInternetGateways',
                'ec2:DescribeNatGateways',
                'ec2:DescribeNetworkAcls',
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
            auto_delete_objects=True,
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
            code=lambda_.Code.from_asset(
                path='./stack/db_api_lambda',
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_8.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pwd && ls -al && pip install -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            timeout=Duration.seconds(900),
            role=db_function_exec_role
        )

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
                'user_agent': 'databricks-CloudFormation-API-caller'
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
                'user_agent': 'databricks-CloudFormation-API-caller'
            }
        )

        create_network = CustomResource(
            self,
            "CreateNetworks",
            service_token=api_function.function_arn,
            properties={
                'action': 'CREATE_NETWORKS',
                'accountId': db_account_id,
                'network_name': workspace_name + '-network',
                'vpc_id': vpc_id,
                'subnet_ids': subnet_ids,
                'security_group_ids': security_group_ids,
                'encodedbase64': base64.b64encode(username.encode('ascii')+b':'+password.encode('ascii')).decode(),
                'user_agent': 'databricks-CloudFormation-API-caller'
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
                'network_id': create_network.get_att_string('NetworkId'),
                'customer_managed_key_id': '',
                'pricing_tier': '',
                'hipaa_parm': '',
                'customer_name': '',
                'authoritative_user_email': '',
                'authoritative_user_full_name': '',
                'user_agent': 'databricks-CloudFormation-API-caller'
            }
        )


