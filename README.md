# databricks_workspace_deployment

This is a PoC deployment for a Databricks workspace using AWS CDK.

## requirements

- docker
- cdk
- aws credentials
- databricks credentials

## usage

export AWS credentials:
```bash
export AWS_DEFAULT_REGION=us-west-2
export AWS_SECRET_ACCESS_KEY=AAAAAAAAAA
export AWS_ACCESS_KEY_ID=BBBBBBBBBB
export AWS_SESSION_TOKEN=CCCCCCCCCC
```
export Databricks parameters:
```bash
export DB_VPC_ID=vpc-xxxxxxxx
export DB_SUBNET_IDS='subnet-xxxxxxxx,subnet-yyyyyyyy'
export DB_SECURITY_GROUP_IDS='sg-xxxxxxxx,sg-yyyyyyyy'
export DB_ACCOUNT_ID='aaaa-bbbb-1234-5678-cccc'
export DB_USERNAME='myemail@domain.com'
export DB_PASSWORD='mypassword'
export DB_WORKSPACE_NAME='My Workspace Name'
```
trigger deployment:
```bash
cdk deploy
```
