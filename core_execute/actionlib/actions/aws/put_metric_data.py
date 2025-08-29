"""Record metric data in AWS CloudWatch"""

from typing import Any, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator

import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util

from core_execute.actionlib.action import BaseAction


class MetricDimension(BaseModel):
    """A single metric dimension for CloudWatch.

    Represents a name-value pair that helps categorize and filter metrics
    in CloudWatch. Dimensions provide additional context for metric data.

    Attributes
    ----------
    name : str
        The name of the dimension (e.g., "Environment", "Application")
    value : str
        The value of the dimension (e.g., "production", "myapp")
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    name: str = Field(..., alias="Name", description="The name of the dimension")
    value: str = Field(..., alias="Value", description="The value of the dimension")


class MetricData(BaseModel):
    """A single metric data point for CloudWatch.

    Represents a complete metric measurement including name, value, unit,
    optional timestamp, and optional dimensions for categorization.

    Attributes
    ----------
    metric_name : str
        The name of the metric (e.g., "ResponseTime", "ErrorCount")
    value : float
        The numeric value of the metric measurement
    unit : str, optional
        The unit of measurement (default: "None")
        Must be a valid CloudWatch unit
    timestamp : str, optional
        ISO 8601 formatted timestamp for the metric data point
        If not provided, CloudWatch uses the current time
    dimensions : list[MetricDimension], optional
        List of dimensions to categorize this metric
        Maximum 10 dimensions per API call
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    metric_name: str = Field(
        ..., alias="MetricName", description="The name of the metric"
    )
    value: float = Field(..., alias="Value", description="The value of the metric")
    unit: str = Field(
        default="None", alias="Unit", description="The unit of the metric"
    )
    timestamp: Optional[datetime | str] = Field(
        default=None,
        alias="Timestamp",
        description="The timestamp for the metric data point (ISO 8601 format or datetime object)",
    )
    dimensions: Optional[list[MetricDimension]] = Field(
        default=None, alias="Dimensions", description="The dimensions for the metric"
    )

    @field_validator("unit")
    @classmethod
    def validate_unit(cls, v: str) -> str:
        """Validate that the unit is a valid CloudWatch unit.

        Parameters
        ----------
        v : str
            The unit value to validate

        Returns
        -------
        str
            The validated unit value

        Raises
        ------
        ValueError
            If the unit is not in the list of valid CloudWatch units
        """
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
            raise ValueError(
                f"Invalid unit '{v}'. Must be one of: {', '.join(sorted(valid_units))}"
            )
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v) -> str | None:
        """Validate and convert timestamp to ISO 8601 string format.

        Parameters
        ----------
        v : str, datetime, or None
            The timestamp value to validate

        Returns
        -------
        str | None
            The validated timestamp as ISO 8601 string, or None

        Raises
        ------
        ValueError
            If timestamp format is invalid
        """
        if v is None:
            return None

        if isinstance(v, str):
            # Validate string format
            try:
                # Parse to ensure valid format, then return original string
                datetime.fromisoformat(v.replace("Z", "+00:00"))
                return v
            except ValueError as e:
                raise ValueError(f"Invalid timestamp format: {e}")

        if isinstance(v, datetime):
            # Convert datetime to ISO 8601 string
            return v.isoformat()

        raise ValueError(
            f"Timestamp must be a string or datetime object, got {type(v)}"
        )


class PutMetricDataActionParams(ActionParams):
    """Parameters for the PutMetricDataAction.

    Contains all configuration needed to record metric data in CloudWatch,
    including target account/region, namespace, and metric definitions.

    Attributes
    ----------
    account : str
        The AWS account ID where metrics will be recorded
    region : str
        The AWS region where metrics will be recorded
    namespace : str
        The CloudWatch namespace for organizing metrics
        Cannot exceed 255 characters or start with 'AWS/'
    metrics : list[MetricData]
        List of metric data points to record
        Maximum 20 metrics per API call
    """

    namespace: str = Field(
        ...,
        alias="Namespace",
        description="The CloudWatch namespace for the metric data",
    )
    metrics: list[MetricData] = Field(
        ..., alias="Metrics", description="List of metric data points to record"
    )

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, v: str) -> str:
        """Validate that namespace follows CloudWatch naming conventions.

        Parameters
        ----------
        v : str
            The namespace value to validate

        Returns
        -------
        str
            The validated namespace value

        Raises
        ------
        ValueError
            If namespace is empty, too long, or uses reserved 'AWS/' prefix
        """
        if not v:
            raise ValueError("Namespace cannot be empty")
        if len(v) > 255:
            raise ValueError("Namespace cannot exceed 255 characters")
        if v.startswith("AWS/"):
            raise ValueError(
                "Namespace cannot start with 'AWS/' (reserved for AWS services)"
            )
        return v

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, v: list[MetricData]) -> list[MetricData]:
        """Validate metrics list against CloudWatch constraints.

        Parameters
        ----------
        v : list[MetricData]
            The metrics list to validate

        Returns
        -------
        list[MetricData]
            The validated metrics list

        Raises
        ------
        ValueError
            If metrics list is empty or exceeds 20 items
        """
        if not v:
            raise ValueError("At least one metric must be provided")
        if len(v) > 20:
            raise ValueError("Cannot submit more than 20 metrics in a single request")
        return v


class PutMetricDataActionSpec(ActionSpec):
    """Generate the action definition for PutMetricData.

    Provides a convenience wrapper for creating PutMetricData actions
    with sensible defaults for common use cases.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate and set default parameters for the PutMetricDataActionSpec.

        Provides sensible defaults for action name, kind, scope, and
        a sample metric configuration using template variables.

        Parameters
        ----------
        values : dict[str, Any]
            The input values dictionary

        Returns
        -------
        dict[str, Any]
            The values dictionary with defaults applied
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "put-metric-data"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::PutMetricData"
        if not values.get(
            "depends_on", values.get("DependsOn")
        ):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Spec")):
            values["params"] = {
                "Account": "{{ deployment.account }}",
                "Region": "{{ deployment.region }}",
                "Namespace": "MyApplication/Deployment",
                "Metrics": [
                    {
                        "MetricName": "DeploymentSuccess",
                        "Value": 1.0,
                        "Unit": "Count",
                        "Dimensions": [
                            {
                                "Name": "Environment",
                                "Value": "{{ deployment.environment }}",
                            },
                            {"Name": "Application", "Value": "{{ app.name }}"},
                        ],
                    }
                ],
            }
        return values


class PutMetricDataAction(BaseAction):
    """Put metric data into AWS CloudWatch.

    This action records custom metric data in AWS CloudWatch for monitoring and alerting.
    It supports multiple metrics with dimensions, custom timestamps, and various units.

    **Key Features:**

    - Record custom application and infrastructure metrics
    - Support for metric dimensions for detailed filtering
    - Batch processing of up to 20 metrics per action
    - Automatic timestamp handling or custom timestamp specification
    - Template variable support for dynamic metric values
    - Comprehensive validation of metric data and CloudWatch limits

    **Use Cases:**

    - Deployment success/failure tracking
    - Application performance metrics
    - Business metrics and KPIs
    - Infrastructure health monitoring
    - Custom alerting triggers

    **Action Parameters:**

    :param Account: AWS account ID where metrics will be recorded
    :type Account: str
    :param Region: AWS region where metrics will be recorded
    :type Region: str
    :param Namespace: CloudWatch namespace for organizing metrics
    :type Namespace: str
    :param Metrics: List of metric data points to record
    :type Metrics: list[dict]

    **Examples:**

    Simple deployment success metric:

    .. code-block:: yaml

        - name: record-deployment-success
          kind: put_metric_data
          params:
            Account: "{{ deployment.account }}"
            Region: "{{ deployment.region }}"
            Namespace: "MyApp/Deployments"
            Metrics:
              - MetricName: "DeploymentSuccess"
                Value: 1
                Unit: "Count"
                Dimensions:
                  - Name: "Environment"
                    Value: "{{ deployment.environment }}"
                  - Name: "Application"
                    Value: "{{ app.name }}"

    Multiple metrics with custom timestamps:

    .. code-block:: yaml

        - name: record-performance-metrics
          kind: put_metric_data
          params:
            Account: "{{ deployment.account }}"
            Region: "{{ deployment.region }}"
            Namespace: "MyApp/Performance"
            Metrics:
              - MetricName: "ResponseTime"
                Value: "{{ test_results.avg_response_time }}"
                Unit: "Milliseconds"
                Timestamp: "{{ test_results.timestamp }}"
              - MetricName: "ThroughputRPS"
                Value: "{{ test_results.requests_per_second }}"
                Unit: "Count/Second"
              - MetricName: "ErrorRate"
                Value: "{{ test_results.error_percentage }}"
                Unit: "Percent"

    **CloudWatch Limits:**

    - Maximum 20 metrics per API call
    - Namespace cannot exceed 255 characters
    - Cannot use 'AWS/' namespace prefix (reserved)
    - Metric names and dimension names cannot exceed 255 characters
    - Maximum 10 dimensions per metric

    **State Tracking:**

    This action tracks execution state using instance-based keys:

    - ``{timestamp}/status`` - Success/error status for each execution
    - ``{timestamp}/metrics_sent`` - Number of metrics recorded
    - ``{timestamp}/batches_processed`` - Number of API batches sent
    - ``status`` - Overall action status
    - ``total_metrics_sent`` - Total metrics recorded across all executions
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the PutMetricDataAction.

        Parameters
        ----------
        definition : ActionSpec
            The action specification containing parameters and configuration
        context : dict[str, Any]
            Template rendering context with deployment variables
        deployment_details : DeploymentDetails
            Deployment context and metadata
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = PutMetricDataActionParams(**definition.params)

        # Processed metric data ready for CloudWatch API
        self.metric_data: list[dict[str, Any]] = []

    def _execute(self):
        """Execute the metric data recording operation.

        Records all configured metrics to CloudWatch using the put_metric_data API.
        Metrics are processed in batches if more than 20 are provided.

        The execution process:

        1. Creates CloudWatch client with appropriate IAM role
        2. Processes metrics in batches of 20 (CloudWatch API limit)
        3. Sends each batch via put_metric_data API call
        4. Tracks execution state and completion status
        5. Records success/error information for monitoring

        Raises
        ------
        Exception
            If CloudWatch API call fails or if metric data is invalid

        Notes
        -----
        This method implements the core functionality and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("PutMetricDataAction._execute()")

        try:
            # Create a unique timestamp for this action execution
            start_time = util.get_current_timestamp()
            datetime_label = start_time.replace(":", "-").replace(".", "-")

            # Track execution in state
            self.set_state("start_time", start_time)
            self.set_state("metrics_count", len(self.metric_data))
            self.set_state("namespace", self.params.namespace)

            # Obtain CloudWatch client
            cloudwatch_client = aws.cloudwatch_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            # Process metrics in batches of 20 (CloudWatch limit)
            batch_size = 20
            total_metrics = len(self.metric_data)
            batches_processed = 0

            for i in range(0, total_metrics, batch_size):
                batch = self.metric_data[i : i + batch_size]

                log.debug(
                    f"Sending batch {batches_processed + 1} with {len(batch)} metrics to CloudWatch"
                )

                # Send batch to CloudWatch
                response = cloudwatch_client.put_metric_data(
                    Namespace=self.params.namespace, MetricData=batch
                )

                batches_processed += 1
                log.debug(
                    f"Successfully sent batch {batches_processed}, response: {response}"
                )

            completion_time = util.get_current_timestamp()
            self.set_state("metrics", self.metric_data)

            # Set general state
            self.set_state("status", "success")
            self.set_state("last_execution_time", completion_time)
            self.set_state("total_metrics_sent", total_metrics)

            log.info(
                f"Successfully recorded {total_metrics} metrics to CloudWatch namespace '{self.params.namespace}' in {batches_processed} batches"
            )

            self.set_complete(
                f"Successfully recorded {total_metrics} metrics to CloudWatch"
            )

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
        """Check operation - not applicable for metric recording.

        CloudWatch metric recording does not support check operations
        as metrics represent point-in-time measurements that cannot
        be verified without actually recording them.

        Raises
        ------
        RuntimeError
            Always raises as check operation is not supported
        """
        log.trace("PutMetricDataAction._check()")
        self.set_failed("Check operation not supported for metric data recording")
        log.trace("PutMetricDataAction._check() complete")

    def _unexecute(self):
        """Unexecute operation - not applicable for metric recording.

        CloudWatch metrics cannot be deleted once recorded, so this is a no-op.
        Metric data becomes part of the CloudWatch time series and cannot
        be removed through the API.

        Notes
        -----
        This is a no-op method as CloudWatch does not support metric deletion.
        """
        log.debug(
            "Unexecute requested for metric data - metrics cannot be deleted from CloudWatch"
        )
        pass

    def _cancel(self):
        """Cancel operation - not applicable for metric recording.

        CloudWatch API calls cannot be cancelled once initiated.
        The put_metric_data operation is atomic and completes quickly.

        Notes
        -----
        This is a no-op method as CloudWatch API calls cannot be cancelled.
        """
        log.debug(
            "Cancel requested for metric data recording - operation cannot be cancelled"
        )
        pass

    def _resolve(self):
        """Resolve template variables and prepare metric data for CloudWatch API.

        Processes all metrics, renders template variables, validates data formats,
        and prepares the final metric data structure for the CloudWatch API.

        **Resolution Process:**

        1. **Template Rendering**: Renders all template variables in account, region, namespace
        2. **Metric Processing**: Processes each metric individually:

           - Renders metric name and value templates
           - Converts values to appropriate numeric types
           - Validates and parses custom timestamps
           - Processes dimension name/value templates

        3. **Data Structuring**: Converts to CloudWatch API format
        4. **Validation**: Ensures all data meets CloudWatch constraints

        **Template Variables Available:**

        - ``deployment.*`` - Deployment context (account, region, environment)
        - ``app.*`` - Application information (name, version, config)
        - ``branch.*`` - Branch details (name, type, commit)
        - ``env.*`` - Environment variables
        - Action outputs from dependencies

        Raises
        ------
        ValueError
            If metric values cannot be converted to numeric types
        Exception
            If template rendering fails or data validation errors occur

        Notes
        -----
        This method prepares data for execution and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("PutMetricDataAction._resolve()")

        try:
            # Render account, region, and namespace
            self.params.account = self.renderer.render_string(
                self.params.account, self.context
            )
            self.params.region = self.renderer.render_string(
                self.params.region, self.context
            )
            self.params.namespace = self.renderer.render_string(
                self.params.namespace, self.context
            )

            # Process each metric
            metric_data = []
            for metric in self.params.metrics:
                # Render template variables in metric name and value
                metric_name = self.renderer.render_string(
                    metric.metric_name, self.context
                )
                metric_value = self.renderer.render_string(
                    str(metric.value), self.context
                )

                # Convert value to float
                try:
                    metric_value_float = float(metric_value)
                except (ValueError, TypeError) as e:
                    raise ValueError(
                        f"Metric '{metric_name}' value '{metric_value}' cannot be converted to float: {e}"
                    )

                # Prepare metric data entry
                metric_entry = {
                    "MetricName": metric_name,
                    "Value": metric_value_float,
                    "Unit": metric.unit,
                }

                # Add timestamp if provided
                if metric.timestamp:
                    timestamp_str = self.renderer.render_string(
                        metric.timestamp, self.context
                    )
                    try:
                        # Parse ISO 8601 timestamp
                        timestamp_dt = datetime.fromisoformat(
                            timestamp_str.replace("Z", "+00:00")
                        )
                        metric_entry["Timestamp"] = timestamp_dt
                    except ValueError as e:
                        log.warn(
                            f"Invalid timestamp format '{timestamp_str}' for metric '{metric_name}': {e}. Using current time."
                        )

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

            log.debug(
                f"Resolved {len(metric_data)} metrics for namespace '{self.params.namespace}'"
            )

        except Exception as e:
            log.error(f"Failed to resolve metric data: {e}")
            self.set_failed(f"Failed to resolve metric data: {e}")
            return

        log.trace("PutMetricDataAction._resolve() complete")

    @classmethod
    def generate_action_spec(cls, **kwargs) -> PutMetricDataActionSpec:
        return PutMetricDataActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> PutMetricDataActionParams:
        return PutMetricDataActionParams(**kwargs)
