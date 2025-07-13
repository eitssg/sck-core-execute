"""Record metric data in AWS CloudWatch"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails

import core_helper.aws as aws

import core_framework as util

from core_execute.actionlib.action import BaseAction


class PutMetricDataActionParams(BaseModel):
    """Parameters for the PutMetricDataAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    namespace: str = Field(
        ...,
        alias="Namespace",
        description="The namespace for the metric data (required)",
    )
    metrics: list[dict[str, Any]] = Field(
        ...,
        alias="Metrics",
        description="A list of metrics to record (required). Each metric should have MetricName, Value, and optionally Unit and DimensionSets.",
    )


class PutMetricActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the PutMetricDataActionSpec"""
        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-aws-putmetricdata-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "AWS::PutMetricData"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "namespace": "",
                "metrics": [
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
            }
        return values


class PutMetricDataAction(BaseAction):
    """Put metrics data into AWS CloudWatch

    This action will put metric data into AWS CloudWatch.  The action will wait for the data to be recorded before returning.

    Attributes:
        Type: Use the value: ``AWS::PutMetricData``
        Params.Account: The account where the metric data is to be recorded
        Params.Region: The region where the metric data is to be recorded
        Params.Namespace: The namespace for the metric data (required)
        Params.Metrics: A list of metrics to record (required)

    .. rubric: ActionSpec:

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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = PutMetricDataActionParams(**definition.params)

        self.metric_data: list[Any] = []

    def _execute(self):

        log.trace("PutMetricDataAction._execute()")

        # Obtain an EC2 client
        cloudwatch_client = aws.cloudwatch_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Put metric data into CloudWatch
        cloudwatch_client.put_metric_data(
            Namespace=self.params.namespace, MetricData=self.metric_data
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

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.namespace = self.renderer.render_string(
            self.params.namespace, self.context
        )

        # Transform metrics with dimension sets to a simple array of metric data
        metric_data = []
        for metric in self.params.metrics:
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
