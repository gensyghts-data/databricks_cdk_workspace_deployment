#!/usr/bin/env python3

from aws_cdk import App
from stack.databricks import Databricks

app = App()
Databricks(app,
           construct_id="DatabricksStack",
           description="CDK config for a Databricks Workspace"
           )

app.synth()
