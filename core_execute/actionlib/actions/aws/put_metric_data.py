"""Record metric data in AWS CloudWatch"""

from typing import Any

import core_logging as log

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util

from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::PutMetricData",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            Namespace="The namespace for the metric data (required)",
            Metrics=[
                {
                    "MetricName": "The name of the metric (required)",
                    "Value": "The value of the metric (required)",
                    "Unit": "The unit of the metric (optional, defaults to 'None')",
                    "DimensionSets": [
                        {
                            "Name": "The name of the dimension (required)",
                            "Value": "The value of the dimension (required)",
                        }
                    ],
                }
            ],
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class PutMetricDataAction(BaseAction):
    """Put metrics data into AWS CloudWatch

    This action will put metric data into AWS CloudWatch.  The action will wait for the data to be recorded before returning.

    Attributes:
        Type: Use the value: ``AWS::PutMetricData``
        Params.Account: The account where the metric data is to be recorded
        Params.Region: The region where the metric data is to be recorded
        Params.Namespace: The namespace for the metric data (required)
        Params.Metrics: A list of metrics to record (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-putmetricdata-label
              Type: "AWS::PutMetricData"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                Namespace: "The namespace for the metric data (required)"
                Metrics:
                  - MetricName: "The name of the metric (required)"
                    Value: "The value of the metric (required)"
                    Unit: "The unit of the metric (optional, defaults to 'None')"
                    DimensionSets:
                  - Name: "The name of the dimension (required)"
                    Value: "The value of the dimension (required)"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.metric_data: list[Any] = []

    def _execute(self):

        log.trace("PutMetricDataAction._execute()")

        # Obtain an EC2 client
        cloudwatch_client = aws.cloudwatch_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Put metric data into CloudWatch
        cloudwatch_client.put_metric_data(
            Namespace=self.params.Namespace, MetricData=self.metric_data
        )

        self.set_complete("Success")

        log.trace("PutMetricDataAction._execute() complete")

    def _check(self):

        log.trace("PutMetricDataAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("PutMetricDataAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("PutMetricDataAction._resolve()")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.Namespace = self.renderer.render_string(
            self.params.Namespace, self.context
        )

        # Transform metrics with dimension sets to a simple array of metric data
        metric_data = []
        for metric in self.params.Metrics:
            for dimensions in metric["DimensionSets"]:
                metric_data.append(
                    {
                        "MetricName": metric["MetricName"],
                        "Value": float(
                            self.renderer.render_string(metric["Value"], self.context)
                        ),
                        "Unit": metric.get("Unit", "None"),
                        "Dimensions": [
                            {
                                "Name": k,
                                "Value": self.renderer.render_string(v, self.context),
                            }
                            for k, v in dimensions.items()
                        ],
                    }
                )

        self.metric_data = metric_data

        log.trace("PutMetricDataAction._resolve()")
