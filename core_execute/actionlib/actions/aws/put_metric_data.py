from typing import Any

from core_framework.models import ActionDefinition, DeploymentDetails

import core_helper.aws as aws

import core_execute.envinfo as envinfo

from core_execute.actionlib.action import BaseAction


class PutMetricDataAction(BaseAction):
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        self.account = self.params.Account
        self.region = self.params.Region
        self.namespace = self.params.Namespace
        self.metrics = self.params.Metrics
        self.metric_data: list[Any] = []  # Populated during resolve

    def _execute(self):
        # Obtain an EC2 client
        cloudwatch_client = aws.cloudwatch_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        # Put metric data into CloudWatch
        cloudwatch_client.put_metric_data(
            Namespace=self.namespace, MetricData=self.metric_data
        )

        self.set_complete("Success")

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.namespace = self.renderer.render_string(self.namespace, self.context)

        # Transform metrics with dimension sets to a simple array of metric data
        metric_data = []
        for metric in self.metrics:
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
