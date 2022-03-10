#!/usr/bin/env python3

from aws_cdk import App
from stack.databricks import Databricks
import os

app = App()
Databricks(app,
           construct_id="DatabricksStack",
           description="CDK config for a Databricks Workspace",
           db_account_id=os.environ['DB_ACCOUNT_ID'],
           username=os.environ['DB_USERNAME'],
           password=os.environ['DB_PASSWORD'],
           workspace_name=os.environ['DB_WORKSPACE_NAME'],
           vpc_id=os.getenv('DB_VPC_ID',''),
           subnet_ids=os.getenv('DB_SUBNET_IDS',''),
           security_group_ids=os.getenv('DB_SECURITY_GROUP_IDS',''),
           )

app.synth()
