"""Record metric data in AWS CloudWatch.

Sends custom metrics (with optional dimensions and timestamps) to a namespace.
"""

from typing import Any, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator

import core_logging as log

from core_framework.models import ActionResource, DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util

from core_execute.actionlib.action import BaseAction


class MetricDimension(BaseModel):
    """CloudWatch metric dimension (name/value)."""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    name: str = Field(..., alias="Name", description="The name of the dimension")
    value: str = Field(..., alias="Value", description="The value of the dimension")


class MetricData(BaseModel):
    """A single CloudWatch metric data point."""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    metric_name: str = Field(..., alias="MetricName", description="The name of the metric")
    value: float = Field(..., alias="Value", description="The value of the metric")
    unit: str = Field(default="None", alias="Unit", description="The unit of the metric")
    timestamp: Optional[datetime | str] = Field(
        default=None,
        alias="Timestamp",
        description="The timestamp for the metric data point (ISO 8601 string or datetime)",
    )
    dimensions: Optional[list[MetricDimension]] = Field(
        default=None, alias="Dimensions", description="The dimensions for the metric"
    )

    @field_validator("unit")
    @classmethod
    def validate_unit(cls, v: str) -> str:
        """Ensure the unit is one of the valid CloudWatch units."""
        valid_units = {
            "Seconds",
            "Microseconds",
            "Milliseconds",
            "Bytes",
            "Kilobytes",
            "Megabytes",
            "Gigabytes",
            "Terabytes",
            "Bits",
            "Kilobits",
            "Megabits",
            "Gigabits",
            "Terabits",
            "Percent",
            "Count",
            "Bytes/Second",
            "Kilobytes/Second",
            "Megabytes/Second",
            "Gigabytes/Second",
            "Terabytes/Second",
            "Bits/Second",
            "Kilobits/Second",
            "Megabits/Second",
            "Gigabits/Second",
            "Terabits/Second",
            "Count/Second",
            "None",
        }
        if v not in valid_units:
            raise ValueError(f"Invalid unit '{v}'. Must be one of: {', '.join(sorted(valid_units))}")
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v) -> str | None:
        """Accept ISO 8601 string or datetime and normalize to ISO string."""
        if v is None:
            return None

        if isinstance(v, str):
            try:
                datetime.fromisoformat(v.replace("Z", "+00:00"))
                return v
            except ValueError as e:
                raise ValueError(f"Invalid timestamp format: {e}")

        if isinstance(v, datetime):
            return v.isoformat()

        raise ValueError(f"Timestamp must be a string or datetime object, got {type(v)}")


class PutMetricDataActionSpec(ActionSpec):
    """Parameters for recording metrics to CloudWatch."""

    namespace: str = Field(
        ...,
        alias="Namespace",
        description="The CloudWatch namespace for the metric data",
    )
    metrics: list[MetricData] = Field(..., alias="Metrics", description="List of metric data points to record")

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, v: str) -> str:
        """Validate namespace length and reserved prefix."""
        if not v:
            raise ValueError("Namespace cannot be empty")
        if len(v) > 255:
            raise ValueError("Namespace cannot exceed 255 characters")
        if v.startswith("AWS/"):
            raise ValueError("Namespace cannot start with 'AWS/' (reserved for AWS services)")
        return v

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, v: list[MetricData]) -> list[MetricData]:
        """Validate metric list count (1..20 per request)."""
        if not v:
            raise ValueError("At least one metric must be provided")
        if len(v) > 20:
            raise ValueError("Cannot submit more than 20 metrics in a single request")
        return v


class PutMetricDataActionResource(ActionResource):
    """Resource model for PutMetricData (forces kind and normalizes spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::PutMetricData"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, PutMetricDataActionSpec):
            values["spec"] = spec.model_dump()

        return values


class PutMetricDataAction(BaseAction[PutMetricDataActionSpec]):
    """Record custom metrics to CloudWatch (batch size up to 20)."""

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters."""
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = PutMetricDataActionSpec.model_validate(definition.spec)

        # Processed metric data ready for CloudWatch API
        self.metric_data: list[dict[str, Any]] = []

    def _execute(self):
        """Send metrics to CloudWatch using put_metric_data (in batches of 20)."""
        log.trace("PutMetricDataAction._execute()")

        try:
            # Create a unique timestamp for this action execution
            start_time = util.get_current_timestamp()
            datetime_label = start_time.replace(":", "-").replace(".", "-")

            # Track execution in state
            self.set_state("start_time", start_time)
            self.set_state("metrics_count", len(self.metric_data))
            self.set_state("namespace", self.spec.namespace)

            # Obtain CloudWatch client
            cloudwatch_client = aws.cloudwatch_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )

            # Process metrics in batches of 20 (CloudWatch limit)
            batch_size = 20
            total_metrics = len(self.metric_data)
            batches_processed = 0

            for i in range(0, total_metrics, batch_size):
                batch = self.metric_data[i : i + batch_size]

                log.debug(f"Sending batch {batches_processed + 1} with {len(batch)} metrics to CloudWatch")

                # Send batch to CloudWatch
                response = cloudwatch_client.put_metric_data(Namespace=self.spec.namespace, MetricData=batch)

                batches_processed += 1
                log.debug(f"Successfully sent batch {batches_processed}, response: {response}")

            completion_time = util.get_current_timestamp()
            self.set_state("metrics", self.metric_data)

            # Set general state
            self.set_state("status", "success")
            self.set_state("last_execution_time", completion_time)
            self.set_state("total_metrics_sent", total_metrics)

            log.info(
                f"Successfully recorded {total_metrics} metrics to CloudWatch namespace '{self.spec.namespace}' in {batches_processed} batches"
            )

            self.set_complete(f"Successfully recorded {total_metrics} metrics to CloudWatch")

        except Exception as e:
            error_time = util.get_current_timestamp()
            error_message = str(e)

            # Set error state
            self.set_state(f"{datetime_label}/status", "error")
            self.set_state(f"{datetime_label}/error_time", error_time)
            self.set_state(f"{datetime_label}/error_message", error_message)

            # Set general error state
            self.set_state("status", "error")
            self.set_state("last_error_time", error_time)
            self.set_state("last_error_message", error_message)

            log.error(f"Failed to record metrics to CloudWatch: {e}")
            self.set_failed(f"Failed to record metrics to CloudWatch: {e}")
            return

        log.trace("PutMetricDataAction._execute() complete")

    def _check(self):
        """Not applicable for metrics; mark check as unsupported."""
        log.trace("PutMetricDataAction._check()")
        self.set_failed("Check operation not supported for metric data recording")
        log.trace("PutMetricDataAction._check() complete")

    def _unexecute(self):
        """No-op; metrics cannot be deleted from CloudWatch."""
        log.debug("Unexecute requested for metric data - metrics cannot be deleted from CloudWatch")

    def _cancel(self):
        """No-op; put_metric_data calls are atomic and cannot be cancelled."""
        log.debug("Cancel requested for metric data recording - operation cannot be cancelled")

    def _resolve(self):
        """Render templates and build CloudWatch MetricData payloads."""
        log.trace("PutMetricDataAction._resolve()")

        try:
            # Render account, region, and namespace
            self.spec.account = self.renderer.render_string(self.spec.account, self.context)
            self.spec.region = self.renderer.render_string(self.spec.region, self.context)
            self.spec.namespace = self.renderer.render_string(self.spec.namespace, self.context)

            # Process each metric
            metric_data = []
            for metric in self.spec.metrics:
                # Render template variables in metric name and value
                metric_name = self.renderer.render_string(metric.metric_name, self.context)
                metric_value = self.renderer.render_string(str(metric.value), self.context)

                # Convert value to float
                try:
                    metric_value_float = float(metric_value)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Metric '{metric_name}' value '{metric_value}' cannot be converted to float: {e}")

                # Prepare metric data entry
                metric_entry = {
                    "MetricName": metric_name,
                    "Value": metric_value_float,
                    "Unit": metric.unit,
                }

                # Add timestamp if provided
                if metric.timestamp:
                    if isinstance(metric.timestamp, datetime):
                        metric_entry["Timestamp"] = metric.timestamp
                        continue
                    timestamp_str = self.renderer.render_string(metric.timestamp, self.context)
                    try:
                        # Parse ISO 8601 timestamp
                        timestamp_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                        metric_entry["Timestamp"] = timestamp_dt
                    except ValueError as e:
                        log.warn(f"Invalid timestamp format '{timestamp_str}' for metric '{metric_name}': {e}. Using current time.")

                # Process dimensions if provided
                if metric.dimensions:
                    dimensions = []
                    for dim in metric.dimensions:
                        dim_name = self.renderer.render_string(dim.name, self.context)
                        dim_value = self.renderer.render_string(dim.value, self.context)
                        dimensions.append({"Name": dim_name, "Value": dim_value})

                    if dimensions:
                        metric_entry["Dimensions"] = dimensions

                metric_data.append(metric_entry)

            self.metric_data = metric_data

            log.debug(f"Resolved {len(metric_data)} metrics for namespace '{self.spec.namespace}'")

        except Exception as e:
            log.error(f"Failed to resolve metric data: {e}")
            self.set_failed(f"Failed to resolve metric data: {e}")
            return

        log.trace("PutMetricDataAction._resolve() complete")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> PutMetricDataActionResource:
        """Factory: create a typed PutMetricDataActionResource."""
        return PutMetricDataActionResource.model_validate(kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> PutMetricDataActionSpec:
        """Factory: create typed PutMetricDataActionSpec."""
        return PutMetricDataActionSpec.model_validate(kwargs)
